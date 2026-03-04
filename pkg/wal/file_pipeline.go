package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/harshithgowdakt/raftlabkv/pkg/fileutil"
)

// filePipeline pre-allocates segment files in a background goroutine.
// When cut() needs a new segment, it gets a ready-to-use file from
// the pipeline instead of allocating synchronously.
type filePipeline struct {
	dir   string
	size  int64
	count int

	filec  chan *fileutil.LockedFile
	errc   chan error
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newFilePipeline(dir string, fileSize int64) *filePipeline {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &filePipeline{
		dir:    dir,
		size:   fileSize,
		filec:  make(chan *fileutil.LockedFile),
		errc:   make(chan error, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	fp.wg.Add(1)
	go fp.run()
	return fp
}

func (fp *filePipeline) run() {
	defer fp.wg.Done()
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.ctx.Done():
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

// Close shuts down the pipeline goroutine and waits for it to exit.
func (fp *filePipeline) Close() error {
	fp.cancel()
	fp.wg.Wait()
	return nil
}
