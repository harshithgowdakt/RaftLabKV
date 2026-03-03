package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/harshithgowda/distributed-key-value-store/pkg/fileutil"
)

// filePipeline pre-allocates segment files in a background goroutine.
// When cut() needs a new segment, it gets a ready-to-use file from
// the pipeline instead of allocating synchronously.
type filePipeline struct {
	dir   string
	size  int64
	count int

	filec chan *fileutil.LockedFile
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

func (fp *filePipeline) run() {
	defer close(fp.donec)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.donec:
			// Pipeline is closing — clean up the pre-allocated file.
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}

// alloc creates and pre-allocates a temporary file.
// Alternates between 0.tmp and 1.tmp to avoid naming collisions.
func (fp *filePipeline) alloc() (*fileutil.LockedFile, error) {
	// Alternate tmp file names.
	name := fmt.Sprintf("%d.tmp", fp.count%2)
	fp.count++

	fpath := filepath.Join(fp.dir, name)
	f, err := fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	if err := fileutil.Preallocate(f.File, fp.size); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Open returns a pre-allocated, locked file ready to use as a new segment.
func (fp *filePipeline) Open() (*fileutil.LockedFile, error) {
	select {
	case f := <-fp.filec:
		return f, nil
	case err := <-fp.errc:
		return nil, err
	}
}

// Close shuts down the pipeline goroutine.
func (fp *filePipeline) Close() error {
	close(fp.donec)
	return nil
}
