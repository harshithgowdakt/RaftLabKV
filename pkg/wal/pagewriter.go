package wal

import "io"

const walPageBytes = 8 * minSectorSize // 4096 bytes

// pageWriter implements a buffered writer that flushes on page boundaries.
// This ensures that complete pages are written to disk, matching typical
// filesystem block sizes for durability.
type pageWriter struct {
	w          io.Writer
	pageOffset int // current position within a page
	buf        []byte
	bufLen     int
}

func newPageWriter(w io.Writer, pageOffset int) *pageWriter {
	return &pageWriter{
		w:          w,
		pageOffset: pageOffset,
		buf:        make([]byte, walPageBytes),
	}
}

func (pw *pageWriter) Write(p []byte) (n int, err error) {
	// If the write fits in the buffer, just buffer it.
	if pw.bufLen+len(p) < walPageBytes-pw.pageOffset {
		copy(pw.buf[pw.bufLen:], p)
		pw.bufLen += len(p)
		return len(p), nil
	}

	// Fill and flush complete pages.
	total := len(p)
	for len(p) > 0 {
		remaining := walPageBytes - pw.pageOffset - pw.bufLen
		if remaining > len(p) {
			remaining = len(p)
		}
		copy(pw.buf[pw.bufLen:], p[:remaining])
		pw.bufLen += remaining
		p = p[remaining:]

		// Flush when we fill a page.
		if pw.pageOffset+pw.bufLen == walPageBytes {
			if err := pw.flush(); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func (pw *pageWriter) Flush() error {
	return pw.flush()
}

func (pw *pageWriter) flush() error {
	if pw.bufLen == 0 {
		return nil
	}
	_, err := pw.w.Write(pw.buf[:pw.bufLen])
	pw.pageOffset = (pw.pageOffset + pw.bufLen) % walPageBytes
	pw.bufLen = 0
	return err
}
