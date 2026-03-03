package wal

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/harshithgowda/distributed-key-value-store/pkg/wal/walpb"
	"google.golang.org/protobuf/proto"
)

// Frame format:
//
//	[frameSize: 8 bytes LE] [record data + padding]
//
// The frameSize field encodes:
//   - Lower 56 bits: size of marshalled protobuf Record
//   - Upper 8 bits:  number of padding bytes (0-7) for 8-byte alignment

const frameSizeBytes = 8

// encoder writes WAL records as framed protobuf messages with CRC integrity.
type encoder struct {
	mu        sync.Mutex
	pw        *pageWriter
	crc       hash.Hash32
	buf       []byte   // reusable marshal buffer
	uint64buf [8]byte  // reusable frame size buffer
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		pw:  newPageWriter(w, pageOffset),
		crc: newCRC(prevCrc),
		buf: make([]byte, 1024*1024), // 1MB initial buffer
	}
}

func newFileEncoder(f *os.File, prevCrc uint32) *encoder {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		offset = 0
	}
	pageOffset := int(offset) % walPageBytes
	return newEncoder(f, prevCrc, pageOffset)
}

// encode writes a single WAL record: frame size prefix + protobuf Record + padding.
func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Update CRC with the record data.
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()

	// Marshal the record.
	data, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	// Grow buffer if needed.
	if len(data) > len(e.buf) {
		e.buf = make([]byte, len(data))
	}

	// Compute frame size with padding info.
	padBytes := paddingLen(len(data))
	frameSize := encodeFrameSize(len(data), padBytes)
	binary.LittleEndian.PutUint64(e.uint64buf[:], frameSize)

	// Write frame size.
	if _, err := e.pw.Write(e.uint64buf[:]); err != nil {
		return err
	}

	// Write record data + padding.
	if padBytes > 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	_, err = e.pw.Write(data)
	return err
}

func (e *encoder) flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pw.Flush()
}

// encodeFrameSize packs the record size and padding into a uint64.
// Lower 56 bits = data size, upper 8 bits = pad bytes.
func encodeFrameSize(dataBytes int, padBytes int) uint64 {
	return uint64(dataBytes) | (uint64(padBytes) << 56)
}

// decodeFrameSize unpacks the record size and padding.
func decodeFrameSize(frame uint64) (dataBytes int64, padBytes int64) {
	dataBytes = int64(frame & 0x00FFFFFFFFFFFFFF)
	padBytes = int64((frame >> 56) & 0xFF)
	return
}

// paddingLen returns the number of pad bytes needed for 8-byte alignment.
func paddingLen(dataBytes int) int {
	rem := dataBytes % 8
	if rem == 0 {
		return 0
	}
	return 8 - rem
}

// CRC helpers using CRC-32C (Castagnoli).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func newCRC(prev uint32) hash.Hash32 {
	h := crc32.New(crcTable)
	// Seed with previous CRC value to chain across segments.
	if prev != 0 {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], prev)
		h.Write(b[:])
	}
	return h
}
