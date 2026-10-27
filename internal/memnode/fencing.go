package memnode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	fencingGenerationPrefix = "fencing-generation-"
	fencingGenerationSuffix = ".state"

	fencingCheckpointBeforeTempCreate     = "before_temp_create"
	fencingCheckpointAfterTempCreate      = "after_temp_create"
	fencingCheckpointAfterWrite           = "after_write"
	fencingCheckpointAfterTempSync        = "after_temp_sync"
	fencingCheckpointAfterPublish         = "after_publish"
	fencingCheckpointAfterNamespaceSync   = "after_namespace_sync"
	fencingCheckpointBeforeRetention      = "before_retention"
	fencingCheckpointAfterRetentionRemove = "after_retention_remove"
	fencingCheckpointAfterRetentionSync   = "after_retention_sync"
	fencingCheckpointAfterRetention       = "after_retention"
	fencingCheckpointBeforeResponse       = "before_response"
)

const maxRetainedFencingGenerations = 3

type fencingGenerationFile struct {
	generation uint64
	path       string
}

type fencingPublicationOutcomeUnknownError struct {
	err error
}

func (e *fencingPublicationOutcomeUnknownError) Error() string {
	return fmt.Sprintf("fencing generation publication outcome unknown: %v", e.err)
}

func (e *fencingPublicationOutcomeUnknownError) Unwrap() error {
	return e.err
}

func fencingPublicationOutcomeUnknown(err error) error {
	if err == nil {
		return nil
	}
	return &fencingPublicationOutcomeUnknownError{err: err}
}

func isFencingPublicationOutcomeUnknown(err error) bool {
	var target *fencingPublicationOutcomeUnknownError
	return errors.As(err, &target)
}

// setFencingPublicationHookForTest injects deterministic publication failures
// at the persistence checkpoints. It is intentionally only used by tests; a
// production node always publishes through the platform barrier below.
func (n *Participant) setFencingPublicationHookForTest(fn func(point string) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fencingPublicationHook = fn
}

func loadFencingGeneration(dir string) (uint64, error) {
	candidates, err := fencingGenerationFiles(dir)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	highest := candidates[0]
	if err := validateFencingGenerationFile(highest.path, highest.generation); err != nil {
		return 0, fmt.Errorf("highest fencing generation is unreadable: %w", err)
	}
	return highest.generation, nil
}

func fencingGenerationFiles(dir string) ([]fencingGenerationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	candidates := make([]fencingGenerationFile, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, fencingGenerationPrefix) ||
			!strings.HasSuffix(name, fencingGenerationSuffix) {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, fencingGenerationPrefix), fencingGenerationSuffix)
		generation, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || generation == 0 {
			continue
		}
		if name != filepath.Base(fencingGenerationPath(dir, generation)) {
			continue
		}
		candidates = append(candidates, fencingGenerationFile{
			generation: generation,
			path:       filepath.Join(dir, name),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].generation > candidates[j].generation
	})
	return candidates, nil
}

func persistFencingGeneration(dir string, generation uint64) error {
	return persistFencingGenerationWithHook(dir, generation, nil)
}

func persistFencingGenerationWithHook(dir string, generation uint64, hook func(string) error) error {
	if generation == 0 {
		return nil
	}
	finalPath := fencingGenerationPath(dir, generation)
	if _, err := os.Stat(finalPath); err == nil {
		if err := validateFencingGenerationFile(finalPath, generation); err != nil {
			return err
		}
		if err := syncFencingNamespace(dir); err != nil {
			return fencingPublicationOutcomeUnknown(fmt.Errorf("sync fencing namespace: %w", err))
		}
		if err := callFencingCheckpoint(hook, fencingCheckpointAfterNamespaceSync); err != nil {
			return fencingPublicationOutcomeUnknown(err)
		}
		return completeFencingPublication(dir, hook)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := callFencingCheckpoint(hook, fencingCheckpointBeforeTempCreate); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".fencing-generation-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()
	if err := temp.Chmod(0o600); err != nil {
		return err
	}
	if err := callFencingCheckpoint(hook, fencingCheckpointAfterTempCreate); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(temp, "%d\n", generation); err != nil {
		return err
	}
	if err := callFencingCheckpoint(hook, fencingCheckpointAfterWrite); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := callFencingCheckpoint(hook, fencingCheckpointAfterTempSync); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	published, publishErr := publishFencingGeneration(tempPath, finalPath)
	if publishErr != nil && !published {
		if validateErr := validateFencingGenerationFile(finalPath, generation); validateErr != nil {
			return publishErr
		}
	}
	if published {
		if err := callFencingCheckpoint(hook, fencingCheckpointAfterPublish); err != nil {
			return fencingPublicationOutcomeUnknown(err)
		}
	}
	if err := syncFencingNamespace(dir); err != nil {
		return fencingPublicationOutcomeUnknown(fmt.Errorf("sync fencing namespace: %w", err))
	}
	if err := callFencingCheckpoint(hook, fencingCheckpointAfterNamespaceSync); err != nil {
		return fencingPublicationOutcomeUnknown(err)
	}
	if publishErr != nil {
		return fencingPublicationOutcomeUnknown(publishErr)
	}
	return completeFencingPublication(dir, hook)
}

func completeFencingPublication(dir string, hook func(string) error) error {
	if err := callFencingCheckpoint(hook, fencingCheckpointBeforeRetention); err != nil {
		return fencingPublicationOutcomeUnknown(err)
	}
	if err := retainFencingGenerations(dir, hook); err != nil {
		return fencingPublicationOutcomeUnknown(fmt.Errorf("retain fencing generations: %w", err))
	}
	if err := callFencingCheckpoint(hook, fencingCheckpointAfterRetention); err != nil {
		return fencingPublicationOutcomeUnknown(err)
	}
	return fencingPublicationOutcomeUnknown(callFencingCheckpoint(hook, fencingCheckpointBeforeResponse))
}

func retainFencingGenerations(dir string, hook func(string) error) error {
	candidates, err := fencingGenerationFiles(dir)
	if err != nil {
		return err
	}
	if len(candidates) <= maxRetainedFencingGenerations {
		if err := callFencingCheckpoint(hook, fencingCheckpointAfterRetentionRemove); err != nil {
			return err
		}
		return callFencingCheckpoint(hook, fencingCheckpointAfterRetentionSync)
	}
	for _, candidate := range candidates[maxRetainedFencingGenerations:] {
		if err := removeFencingGeneration(candidate.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := callFencingCheckpoint(hook, fencingCheckpointAfterRetentionRemove); err != nil {
			return err
		}
	}
	if err := syncFencingNamespace(dir); err != nil {
		return err
	}
	return callFencingCheckpoint(hook, fencingCheckpointAfterRetentionSync)
}

func callFencingCheckpoint(hook func(string) error, point string) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

func fencingGenerationPath(dir string, generation uint64) string {
	return filepath.Join(
		dir,
		fmt.Sprintf("%s%020d%s", fencingGenerationPrefix, generation, fencingGenerationSuffix),
	)
}

func validateFencingGenerationFile(path string, expected uint64) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	actual, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if actual != expected {
		return fmt.Errorf("%s contains generation %d, want %d", path, actual, expected)
	}
	return nil
}
