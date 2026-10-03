/*
Copyright 2026 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	artcfg "github.com/fluxcd/pkg/artifact/config"
	artstore "github.com/fluxcd/pkg/artifact/storage"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

func testShardGCObjectMeta(namespace, name string, lbls map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: lbls}
}

func testShardGCGitRepository(namespace, name string, lbls map[string]string) *sourcev1.GitRepository {
	return &sourcev1.GitRepository{ObjectMeta: testShardGCObjectMeta(namespace, name, lbls)}
}

func testShardGCHelmChart(namespace, name string, lbls map[string]string) *sourcev1.HelmChart {
	return &sourcev1.HelmChart{ObjectMeta: testShardGCObjectMeta(namespace, name, lbls)}
}

// newTestShardArtifactGC returns a ShardArtifactGC backed by a fake API
// reader and a real storage rooted at a temp dir.
func newTestShardArtifactGC(t *testing.T, objs []client.Object, selector labels.Selector, namespace string) (*ShardArtifactGC, string) {
	t.Helper()

	s := runtime.NewScheme()
	if err := sourcev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	apiReader := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()

	dir := t.TempDir()
	st, err := artstore.New(&artcfg.Options{StoragePath: dir, StorageAdvAddress: "localhost"})
	if err != nil {
		t.Fatal(err)
	}

	return &ShardArtifactGC{
		Storage:   st,
		APIReader: apiReader,
		Selector:  selector,
		Namespace: namespace,
		Interval:  time.Minute,
		Log:       logr.Discard(),
	}, dir
}

// writeTestArtifactDir creates '<kind>/<namespace>/<name>/data.tar.gz'
// under base.
func writeTestArtifactDir(t *testing.T, base, kind, namespace, name string) {
	t.Helper()
	dir := filepath.Join(base, kind, namespace, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.tar.gz"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertDirExistence(t *testing.T, base string, dirs [][3]string, wantExist bool) {
	t.Helper()
	for _, d := range dirs {
		p := filepath.Join(base, d[0], d[1], d[2])
		_, err := os.Stat(p)
		if wantExist && os.IsNotExist(err) {
			t.Errorf("expected artifact dir %s to exist", p)
		}
		if !wantExist && err == nil {
			t.Errorf("expected artifact dir %s to be garbage collected", p)
		}
	}
}

func TestShardArtifactGC_collect(t *testing.T) {
	selector, err := labels.Parse("sharding.fluxcd.io/key=shard1")
	if err != nil {
		t.Fatal(err)
	}
	shard1 := map[string]string{"sharding.fluxcd.io/key": "shard1"}
	shard2 := map[string]string{"sharding.fluxcd.io/key": "shard2"}

	tests := []struct {
		name      string
		objs      []client.Object
		namespace string
		dirs      [][3]string
		wantKept  [][3]string
		wantGone  [][3]string
	}{
		{
			name: "keeps artifacts of objects owned by the shard",
			objs: []client.Object{
				testShardGCGitRepository("default", "podinfo", shard1),
			},
			dirs:     [][3]string{{"gitrepository", "default", "podinfo"}},
			wantKept: [][3]string{{"gitrepository", "default", "podinfo"}},
		},
		{
			name: "removes artifacts when the shard label changed",
			objs: []client.Object{
				testShardGCGitRepository("default", "podinfo", shard2),
			},
			dirs:     [][3]string{{"gitrepository", "default", "podinfo"}},
			wantGone: [][3]string{{"gitrepository", "default", "podinfo"}},
		},
		{
			name: "removes artifacts when the shard label was removed",
			objs: []client.Object{
				testShardGCGitRepository("default", "podinfo", nil),
			},
			dirs:     [][3]string{{"gitrepository", "default", "podinfo"}},
			wantGone: [][3]string{{"gitrepository", "default", "podinfo"}},
		},
		{
			name:     "removes artifacts of objects deleted from the API",
			objs:     nil,
			dirs:     [][3]string{{"gitrepository", "default", "podinfo"}},
			wantGone: [][3]string{{"gitrepository", "default", "podinfo"}},
		},
		{
			name: "leaves directories of unknown kinds alone",
			objs: nil,
			dirs: [][3]string{
				{"weirdkind", "default", "podinfo"},
				{"gitrepository", "default", "podinfo"},
			},
			wantKept: [][3]string{{"weirdkind", "default", "podinfo"}},
			wantGone: [][3]string{{"gitrepository", "default", "podinfo"}},
		},
		{
			name: "respects namespace scope",
			objs: []client.Object{
				testShardGCGitRepository("ns-a", "podinfo", shard1),
				testShardGCGitRepository("ns-b", "podinfo", shard1),
			},
			namespace: "ns-b",
			dirs: [][3]string{
				{"gitrepository", "ns-a", "podinfo"},
				{"gitrepository", "ns-b", "podinfo"},
			},
			wantKept: [][3]string{{"gitrepository", "ns-b", "podinfo"}},
			wantGone: [][3]string{{"gitrepository", "ns-a", "podinfo"}},
		},
		{
			name: "collects across kinds",
			objs: []client.Object{
				testShardGCGitRepository("default", "podinfo", shard1),
				testShardGCHelmChart("default", "podinfo", shard2),
			},
			dirs: [][3]string{
				{"gitrepository", "default", "podinfo"},
				{"helmchart", "default", "podinfo"},
			},
			wantKept: [][3]string{{"gitrepository", "default", "podinfo"}},
			wantGone: [][3]string{{"helmchart", "default", "podinfo"}},
		},
		{
			name:     "empty storage is a no-op",
			objs:     nil,
			dirs:     nil,
			wantKept: nil,
			wantGone: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, base := newTestShardArtifactGC(t, tt.objs, selector, tt.namespace)
			for _, d := range tt.dirs {
				writeTestArtifactDir(t, base, d[0], d[1], d[2])
			}

			if err := g.collect(context.Background()); err != nil {
				t.Fatalf("collect() returned unexpected error: %v", err)
			}

			assertDirExistence(t, base, tt.wantKept, true)
			assertDirExistence(t, base, tt.wantGone, false)
		})
	}
}

func TestShardArtifactGC_Start_validation(t *testing.T) {
	selector := labels.Everything()

	t.Run("requires storage, API reader and selector", func(t *testing.T) {
		g := &ShardArtifactGC{Interval: time.Minute, Log: logr.Discard()}
		if err := g.Start(context.Background()); err == nil {
			t.Error("expected error for missing storage, API reader and selector")
		}
	})

	t.Run("requires positive interval", func(t *testing.T) {
		g, _ := newTestShardArtifactGC(t, nil, selector, "")
		g.Interval = 0
		if err := g.Start(context.Background()); err == nil {
			t.Error("expected error for non-positive interval")
		}
	})

	t.Run("stops on cancelled context", func(t *testing.T) {
		g, _ := newTestShardArtifactGC(t, nil, selector, "")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := g.Start(ctx); err != nil {
			t.Errorf("expected nil error for cancelled context, got: %v", err)
		}
	})
}

func TestShardArtifactGC_NeedLeaderElection(t *testing.T) {
	g := &ShardArtifactGC{}
	if !g.NeedLeaderElection() {
		t.Error("expected NeedLeaderElection() to be true")
	}
}
