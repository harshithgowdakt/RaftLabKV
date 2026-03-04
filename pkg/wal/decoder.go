package wal

import (
	"bufio"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"sync"

	"github.com/harshithgowdakt/raftlabkv/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

const minSectorSize = 512

// decoder reads WAL records from one or more segment files sequentially.
type decoder struct {
	mu           sync.Mutex
	brs          []*bufio.Reader // one reader per WAL segment file
	crc          hash.Hash32
	lastValidOff int64 // byte offset of the end of the last valid record
}

func newDecoder(readers ...io.Reader) *decoder {
	brs := make([]*bufio.Reader, len(readers))
	for i, r := range readers {
		brs[i] = bufio.NewReaderSize(r, 128*1024)
	}
	return &decoder{
		brs: brs,
		crc: crc32.New(crcTable),
	}
}

// decode reads the next WAL record. When a segment is exhausted it
// advances to the next one. Returns io.EOF when all segments are read.
func (d *decoder) decode(rec *walpb.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {
		return io.EOF
	}

	// Read frame size (8 bytes).
	var frameBuf [frameSizeBytes]byte
	_, err := io.ReadFull(d.brs[0], frameBuf[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Try next segment.
			d.brs = d.brs[1:]
			if len(d.brs) == 0 {
				return io.EOF
			}
			d.lastValidOff = 0
			return d.decodeRecord(rec)
		}
		return err
	}

	frameSize := binary.LittleEndian.Uint64(frameBuf[:])

	// A zero frame size means we've hit pre-allocated space (end of data).
	if frameSize == 0 {
		// Advance to next segment.
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.lastValidOff = 0
		return d.decodeRecord(rec)
	}

	dataBytes, padBytes := decodeFrameSize(frameSize)

	// Read record data + padding.
	total := dataBytes + padBytes
	data := make([]byte, total)
	_, err = io.ReadFull(d.brs[0], data)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// Possible torn write — check if remaining data is zeros.
			if isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
		}
		return err
	}

	// Unmarshal the record (only the non-padded portion).
	rec.Reset()
	if err := proto.Unmarshal(data[:dataBytes], rec); err != nil {
		// Check for torn entry (zero-filled sectors).
		if isTornEntry(data[:dataBytes]) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// Validate CRC.
	d.crc.Write(rec.Data)
	if err := rec.Validate(d.crc.Sum32()); err != nil {
		// Check for torn entry before declaring corruption.
		if isTornEntry(data[:dataBytes]) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	// Record is valid — advance lastValidOff.
	d.lastValidOff += frameSizeBytes + total
	return nil
}

// updateCRC reseeds the decoder's CRC hash with a previous CRC value.
// Called when a CrcType record is encountered at a segment boundary.
func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = newCRC(prevCrc)
}

// lastOffset returns the byte offset past the last valid record in the
// current segment. Used by repair/recovery to know where to truncate.
func (d *decoder) lastOffset() int64 { return d.lastValidOff }

// lastCRC returns the current running CRC value.
func (d *decoder) lastCRC() uint32 { return d.crc.Sum32() }

// isTornEntry detects torn writes by checking if sector-aligned chunks
// of the data are all zeros. When a file is pre-allocated with zeros,
// a crash mid-write leaves zero-filled sectors in the unwritten portion.
func isTornEntry(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	// Check if any minSectorSize-aligned chunk from the end is all zeros.
	for i := len(data) - minSectorSize; i >= 0; i -= minSectorSize {
		chunk := data[i : i+minSectorSize]
		allZero := true
		for _, b := range chunk {
			if b != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return true
		}
	}

	// Also check if the remaining tail is all zeros.
	tail := data[len(data)-(len(data)%minSectorSize):]
	if len(tail) > 0 {
		allZero := true
		for _, b := range tail {
			if b != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return true
		}
	}

	return false
}
