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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/fluxcd/pkg/artifact/storage"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

var (
	_ manager.Runnable               = &ShardArtifactGC{}
	_ manager.LeaderElectionRunnable = &ShardArtifactGC{}
)

// ShardArtifactGC is a manager.Runnable that periodically garbage-collects
// stored artifacts for objects this shard no longer owns.
//
// When the manager watches a label-selected subset of the source kinds
// (--watch-label-selector), an object whose labels stop matching the selector
// disappears from the informer cache without being deleted. The reconcilers
// then observe a NotFound and return early, while the artifact files stay on
// disk and the artifact server keeps serving them. This collector reconciles
// the on-disk state with the set of objects the shard owns and removes the
// artifacts of objects that left the shard, or that were deleted while
// invisible to it.
//
// The zero value is not usable; all fields except Namespace and Log must be
// set. It is only added to the manager when a watch label selector is
// configured, so the non-sharded behavior is unchanged.
type ShardArtifactGC struct {
	// Storage is the artifact storage to garbage-collect.
	Storage *storage.Storage
	// APIReader reads objects directly from the API server, bypassing the
	// label-filtered informer cache.
	APIReader client.Reader
	// Selector is the shard's watch label selector. Objects whose labels do
	// not match it are considered to have left the shard.
	Selector labels.Selector
	// Namespace limits ownership to a single namespace, mirroring a
	// namespace-scoped watch. Empty watches all namespaces.
	Namespace string
	// Interval between collection runs.
	Interval time.Duration
	// Log is the logger for collection runs. Defaults to a discard logger.
	Log logr.Logger
}

// NeedLeaderElection implements manager.LeaderElectionRunnable. Artifacts are
// only written by the leader, so collection must only run on the leader.
func (g *ShardArtifactGC) NeedLeaderElection() bool {
	return true
}

// Start implements manager.Runnable. It runs an initial collection and then
// repeats it on every tick of Interval until the context is cancelled.
// Collection failures are logged and retried on the next tick; they never
// stop the runnable.
func (g *ShardArtifactGC) Start(ctx context.Context) error {
	if g.Storage == nil || g.APIReader == nil || g.Selector == nil {
		return fmt.Errorf("storage, API reader and selector are required")
	}
	if g.Interval <= 0 {
		return fmt.Errorf("a positive interval is required")
	}
	log := g.Log
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	log.Info("starting shard artifact garbage collector", "interval", g.Interval.String())
	if err := g.collect(ctx); err != nil {
		log.Error(err, "initial artifact garbage collection failed")
	}

	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := g.collect(ctx); err != nil {
				log.Error(err, "artifact garbage collection failed")
			}
		}
	}
}

// collect removes the stored artifacts of every object directory that is not
// owned by this shard. A directory '<kind>/<namespace>/<name>' is owned when
// the object exists in the API (in the watched namespace, if any) and its
// labels match the shard's watch selector.
func (g *ShardArtifactGC) collect(ctx context.Context) error {
	owned, err := g.listOwnedObjects(ctx)
	if err != nil {
		return err
	}

	var errs []error
	walkErr := filepath.WalkDir(g.Storage.BasePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// A transient read error must not abort the whole run;
			// the next tick retries.
			return nil
		}
		rel, err := filepath.Rel(g.Storage.BasePath, path)
		if err != nil || rel == "." {
			return nil
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 3 || !d.IsDir() {
			// Not an artifact directory ('<kind>/<namespace>/<name>');
			// keep descending, without following anything deeper than
			// an artifact directory.
			if len(parts) >= 3 {
				return filepath.SkipDir
			}
			return nil
		}
		if err := g.collectDir(parts, owned); err != nil {
			errs = append(errs, err)
		}
		return filepath.SkipDir
	})
	if walkErr != nil {
		errs = append(errs, fmt.Errorf("failed to walk artifact storage: %w", walkErr))
	}
	if len(errs) > 0 {
		return fmt.Errorf("shard artifact garbage collection encountered %d errors, first: %w", len(errs), errs[0])
	}
	return nil
}

// collectDir garbage-collects the artifact directory described by parts
// ('<kind>/<namespace>/<name>') when the object is not owned by this shard.
// Directories of unknown kinds are left alone.
func (g *ShardArtifactGC) collectDir(parts []string, owned map[string]labels.Set) error {
	kind, namespace, name := parts[0], parts[1], parts[2]
	if _, ok := shardGCSourceKinds[kind]; !ok {
		return nil
	}

	key := strings.Join(parts, "/")
	lbls, ok := owned[key]
	var reason string
	switch {
	case !ok:
		reason = "object not found"
	case !g.Selector.Matches(labels.Set(lbls)):
		reason = "object left the shard"
	default:
		return nil
	}

	artifact := g.Storage.NewArtifactFor(kind, &metav1.ObjectMeta{Namespace: namespace, Name: name}, "", "*")
	deletedDir, err := g.Storage.RemoveAll(artifact)
	if err != nil {
		return fmt.Errorf("failed to garbage collect artifacts for %s %s/%s: %w", kind, namespace, name, err)
	}
	if deletedDir != "" {
		g.log().Info("garbage collected artifacts for object outside shard",
			"kind", kind, "namespace", namespace, "name", name, "reason", reason)
	}
	return nil
}

// log returns the configured logger, or a discard logger when unset.
func (g *ShardArtifactGC) log() logr.Logger {
	if g.Log.GetSink() == nil {
		return logr.Discard()
	}
	return g.Log
}

// shardGCSourceKinds maps the lowercase storage directory name of a source
// kind to the list type used to enumerate its objects.
var shardGCSourceKinds = map[string]client.ObjectList{
	"gitrepository":  &sourcev1.GitRepositoryList{},
	"helmrepository": &sourcev1.HelmRepositoryList{},
	"helmchart":      &sourcev1.HelmChartList{},
	"bucket":         &sourcev1.BucketList{},
	"ocirepository":  &sourcev1.OCIRepositoryList{},
}

// listOwnedObjects returns the labels of every source object visible to the
// API reader, keyed by storage directory ('<kind>/<namespace>/<name>').
func (g *ShardArtifactGC) listOwnedObjects(ctx context.Context) (map[string]labels.Set, error) {
	owned := make(map[string]labels.Set)
	for kind, list := range shardGCSourceKinds {
		listOpts := []client.ListOption{}
		if g.Namespace != "" {
			listOpts = append(listOpts, client.InNamespace(g.Namespace))
		}
		if err := g.APIReader.List(ctx, list, listOpts...); err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", kind, err)
		}
		items, err := apimeta.ExtractList(list)
		if err != nil {
			return nil, fmt.Errorf("failed to extract %s items: %w", kind, err)
		}
		for _, item := range items {
			obj, ok := item.(metav1.Object)
			if !ok {
				continue
			}
			owned[strings.Join([]string{kind, obj.GetNamespace(), obj.GetName()}, "/")] = obj.GetLabels()
		}
	}
	return owned, nil
}
