package wal

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/harshithgowda/distributed-key-value-store/pkg/fileutil"
	"github.com/harshithgowda/distributed-key-value-store/pkg/wal/walpb"
)

// Repair attempts to fix a corrupted WAL by truncating the last segment
// to the last valid record boundary. Only the last segment can be
// repaired (corruption in earlier segments is unrecoverable).
// A backup of the original file is saved with a ".broken" suffix.
func Repair(dirpath string) bool {
	// Find WAL files.
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		log.Printf("wal repair: read dir: %v", err)
		return false
	}

	var walNames []string
	for _, name := range names {
		if strings.HasSuffix(name, ".wal") {
			walNames = append(walNames, name)
		}
	}
	if len(walNames) == 0 {
		log.Printf("wal repair: no wal files found")
		return false
	}

	// Only repair the last segment.
	lastFile := walNames[len(walNames)-1]
	lastPath := filepath.Join(dirpath, lastFile)

	// Open and decode to find last valid offset.
	f, err := os.Open(lastPath)
	if err != nil {
		log.Printf("wal repair: open %s: %v", lastFile, err)
		return false
	}

	dec := newDecoder(f)
	var rec walpb.Record
	for {
		err := dec.decode(&rec)
		if err == io.EOF {
			// File is fine — nothing to repair.
			f.Close()
			log.Printf("wal repair: %s is not corrupted", lastFile)
			return true
		}
		if err != nil {
			break
		}
	}
	lastOff := dec.lastOffset()
	f.Close()

	if lastOff == 0 {
		log.Printf("wal repair: no valid records in %s", lastFile)
		return false
	}

	log.Printf("wal repair: truncating %s to offset %d", lastFile, lastOff)

	// Create backup.
	brokenPath := lastPath + ".broken"
	if err := os.Rename(lastPath, brokenPath); err != nil {
		log.Printf("wal repair: backup rename: %v", err)
		return false
	}

	// Copy valid portion to a new file.
	src, err := os.Open(brokenPath)
	if err != nil {
		log.Printf("wal repair: open broken: %v", err)
		return false
	}
	defer src.Close()

	dst, err := os.Create(lastPath)
	if err != nil {
		log.Printf("wal repair: create repaired: %v", err)
		return false
	}

	if _, err := io.CopyN(dst, src, lastOff); err != nil {
		dst.Close()
		log.Printf("wal repair: copy: %v", err)
		return false
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		log.Printf("wal repair: sync: %v", err)
		return false
	}
	dst.Close()

	log.Printf("wal repair: successfully repaired %s (backup at %s)", lastFile, filepath.Base(brokenPath))
	return true
}
