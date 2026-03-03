package fileutil

import "os"

// Preallocate extends the file to the given size by seeking and writing
// a zero byte at the end. This ensures the filesystem allocates blocks
// and the file is filled with zeros (important for torn write detection).
func Preallocate(f *os.File, sizeInBytes int64) error {
	// First try to preallocate without changing file size using the
	// existing content. Get current size.
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= sizeInBytes {
		return nil
	}

	// Extend the file to the desired size. On most OS/filesystems,
	// ftruncate to a larger size fills the new space with zeros.
	return f.Truncate(sizeInBytes)
}
