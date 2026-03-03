package walpb

import "errors"

var ErrCRCMismatch = errors.New("walpb: crc mismatch")

// Validate checks that the record's CRC matches the expected value.
func (r *Record) Validate(crc uint32) error {
	if r.Crc != crc {
		return ErrCRCMismatch
	}
	return nil
}
