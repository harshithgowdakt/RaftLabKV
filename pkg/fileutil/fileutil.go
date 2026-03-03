package fileutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const PrivateFileMode = os.FileMode(0640)
const PrivateDirMode = os.FileMode(0750)

// Exist returns true if the path exists.
func Exist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DirEmpty returns true if the directory is empty or does not exist.
func DirEmpty(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return true
	}
	return len(entries) == 0
}

// ReadDir returns sorted filenames in the directory.
func ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// TouchDirAll creates a directory if it doesn't exist.
func TouchDirAll(dir string) error {
	return os.MkdirAll(dir, PrivateDirMode)
}

// CreateDirAll creates a directory and fsyncs the parent.
func CreateDirAll(dir string) error {
	if err := TouchDirAll(dir); err != nil {
		return err
	}
	return Fsync(filepath.Dir(dir))
}

// Fsync fsyncs a file or directory.
func Fsync(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// ZeroToEnd writes zeros from the given offset to the end of the file.
func ZeroToEnd(f *os.File, offset int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	remaining := info.Size() - offset
	if remaining <= 0 {
		return nil
	}

	if _, err := f.Seek(offset, 0); err != nil {
		return err
	}

	// Write zeros in chunks.
	buf := make([]byte, 4096)
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return f.Sync()
}

// CheckWALNames verifies that all names follow the WAL naming convention.
func CheckWALNames(names []string) []string {
	var result []string
	for _, name := range names {
		if isValidWALName(name) {
			result = append(result, name)
		}
	}
	return result
}

func isValidWALName(name string) bool {
	if !strings.HasSuffix(name, ".wal") {
		return false
	}
	base := strings.TrimSuffix(name, ".wal")
	parts := strings.SplitN(base, "-", 2)
	if len(parts) != 2 {
		return false
	}
	if len(parts[0]) != 16 || len(parts[1]) != 16 {
		return false
	}
	var seq, idx uint64
	_, err := fmt.Sscanf(parts[0], "%016x", &seq)
	if err != nil {
		return false
	}
	_, err = fmt.Sscanf(parts[1], "%016x", &idx)
	return err == nil
}
