package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
)

const (
	gobRecordHeaderSize = 12 // Four-byte magic followed by an eight-byte payload length.
	gobRecordMagic      = "CGB1"
)

var errIncompleteGobRecord = errors.New("incomplete gob log record")

// Each record is a separate gob stream. Appending after reopening or compacting
// a log must not depend on type IDs from an earlier encoder.
func encodeGobRecord(value any) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(make([]byte, gobRecordHeaderSize))
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	copy(data, gobRecordMagic)
	binary.BigEndian.PutUint64(data[len(gobRecordMagic):gobRecordHeaderSize], uint64(len(data)-gobRecordHeaderSize))
	return data, nil
}

func writeGobRecord(w io.Writer, value any) error {
	data, err := encodeGobRecord(value)
	if err != nil {
		return err
	}
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// The section covers the file as it was opened. Check lengths against the
// remaining bytes before allocating, including when a damaged header is huge.
// Only an incomplete frame is recoverable as a torn replay tail; a complete
// frame with invalid gob data is corruption, even if gob reports unexpected EOF.
func readGobRecord(r *io.SectionReader, value any) error {
	var header [gobRecordHeaderSize]byte
	n, err := io.ReadFull(r, header[:])
	if n == 0 && err == io.EOF {
		return io.EOF
	}
	prefixLen := min(n, len(gobRecordMagic))
	if string(header[:prefixLen]) != gobRecordMagic[:prefixLen] {
		return errors.New("unsupported gob log format")
	}
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return errIncompleteGobRecord
		}
		return err
	}
	length := binary.BigEndian.Uint64(header[len(gobRecordMagic):])
	if length == 0 {
		return errors.New("empty gob log record")
	}
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if length > uint64(r.Size()-offset) {
		return errIncompleteGobRecord
	}
	if length > uint64(^uint(0)>>1) {
		return errors.New("gob log record exceeds addressable memory")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	data := bytes.NewReader(payload)
	if err := gob.NewDecoder(data).Decode(value); err != nil {
		return fmt.Errorf("invalid gob log record: %w", err)
	}
	if data.Len() != 0 {
		return errors.New("trailing data in gob log record")
	}
	return nil
}
