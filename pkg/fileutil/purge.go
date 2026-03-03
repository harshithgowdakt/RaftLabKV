package fileutil

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const defaultPurgeInterval = 30 * time.Second

// PurgeFile periodically removes old files matching the given suffix in dir,
// keeping at most max files. It attempts to flock each file before removing
// (safe for WAL files that may still be held by the WAL). Runs until stop
// is closed. Returns a done channel that is closed when the goroutine exits.
func PurgeFile(dir, suffix string, max uint, stop <-chan struct{}) <-chan struct{} {
	return PurgeFileWithInterval(dir, suffix, max, defaultPurgeInterval, stop)
}

// PurgeFileWithInterval is like PurgeFile but with a configurable interval.
func PurgeFileWithInterval(dir, suffix string, max uint, interval time.Duration, stop <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				purgeOnce(dir, suffix, max, true)
			case <-stop:
				return
			}
		}
	}()
	return done
}

// PurgeFileWithoutFlock is like PurgeFile but does not attempt to
// acquire a lock before deleting. Used for snapshot files which are
// not locked.
func PurgeFileWithoutFlock(dir, suffix string, max uint, stop <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultPurgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				purgeOnce(dir, suffix, max, false)
			case <-stop:
				return
			}
		}
	}()
	return done
}

func purgeOnce(dir, suffix string, max uint, useFlock bool) {
	names, err := ReadDir(dir)
	if err != nil {
		return
	}

	// Filter to matching suffix.
	var matching []string
	for _, name := range names {
		if strings.HasSuffix(name, suffix) {
			matching = append(matching, name)
		}
	}
	sort.Strings(matching)

	// Keep only max files (the newest by name).
	if uint(len(matching)) <= max {
		return
	}

	toRemove := matching[:len(matching)-int(max)]
	for _, name := range toRemove {
		fpath := filepath.Join(dir, name)
		if useFlock {
			// Try to lock before removing — skip if held by another process.
			lf, err := TryLockFile(fpath, os.O_WRONLY, PrivateFileMode)
			if err != nil {
				continue // file still in use
			}
			lf.Close()
		}
		if err := os.Remove(fpath); err != nil {
			log.Printf("fileutil: failed to purge %s: %v", name, err)
		}
	}
}
