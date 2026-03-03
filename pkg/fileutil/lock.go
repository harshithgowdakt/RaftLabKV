package fileutil

import (
	"os"
	"syscall"
)

// LockedFile is an os.File with an associated advisory file lock.
type LockedFile struct {
	*os.File
}

// TryLockFile attempts to acquire an exclusive advisory lock on the
// file at the given path, opening it with the specified flags. Returns
// an error immediately if the lock is already held by another process.
func TryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{File: f}, nil
}

// LockFile acquires an exclusive advisory lock on the file, blocking
// until the lock is available.
func LockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{File: f}, nil
}

// Close releases the lock and closes the file.
func (lf *LockedFile) Close() error {
	// Closing the fd automatically releases the flock.
	return lf.File.Close()
}
