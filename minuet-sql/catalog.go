package minuetsql

import (
	"bytes"
	"encoding/binary"

	minuetindex "github.com/jbancroft/ConcertumDemo/minuet-index"
)

const (
	catalogAddr               = 0
	catalogFormatVersion      = uint32(2)
	catalogHeaderSize         = 32
	catalogMagic              = uint32(0x4d53514c) // MSQL
	catalogMaxNameLen         = catalogRecordSize - catalogRecordNameOffset
	catalogMaxRecords         = (catalogSize - catalogHeaderSize) / catalogRecordSize
	catalogRecordSize         = 80
	catalogSize               = 512
	defaultCatalogNextBase    = 1024
	minuetIndexSuperblockSize = 64
)

// Byte offsets in the catalog header and table records.
const (
	catalogNextBaseAddrOffset     = 16
	catalogNextTableIDOffset      = 8
	catalogRecordBaseAddrOffset   = 8
	catalogRecordNameOffset       = 24
	catalogRecordRegionSizeOffset = 16
	catalogTableCountOffset       = 24
	catalogVersionOffset          = 4
)

type catalogState struct {
	nextTableID  uint64
	nextBaseAddr uint64
	tables       []catalogTableRecord
}

type catalogTableRecord struct {
	tableID       uint64
	name          string
	indexBaseAddr uint64
	regionSize    uint64
}

func decodeCatalog(raw []byte, memorySize uint64) (catalogState, error) {
	if len(raw) != catalogSize || memorySize < defaultCatalogNextBase {
		return catalogState{}, ErrInvalidCatalog
	}
	out := catalogState{nextTableID: 1, nextBaseAddr: defaultCatalogNextBase}
	if bytes.Equal(raw, make([]byte, catalogSize)) {
		return out, nil
	}
	if binary.BigEndian.Uint32(raw) != catalogMagic ||
		binary.BigEndian.Uint32(raw[catalogVersionOffset:]) != catalogFormatVersion {
		return catalogState{}, ErrInvalidCatalog
	}
	tableCount := binary.BigEndian.Uint32(raw[catalogTableCountOffset:])
	if tableCount > catalogMaxRecords {
		return catalogState{}, ErrInvalidCatalog
	}
	for i := range tableCount {
		off := catalogHeaderSize + int(i)*catalogRecordSize
		r := raw[off : off+catalogRecordSize]
		rec := catalogTableRecord{
			tableID:       binary.BigEndian.Uint64(r),
			indexBaseAddr: binary.BigEndian.Uint64(r[catalogRecordBaseAddrOffset:]),
			regionSize:    binary.BigEndian.Uint64(r[catalogRecordRegionSizeOffset:]),
			name:          string(bytes.TrimRight(r[catalogRecordNameOffset:], "\x00")),
		}
		// Tables are append-only. Their regions must form the same contiguous
		// layout on reopen, with no overlap, overflow, or escape from the node.
		name, err := parseIdentifier(rec.name)
		if err != nil || name != rec.name || rec.tableID != out.nextTableID ||
			rec.indexBaseAddr != out.nextBaseAddr || rec.regionSize < minuetindex.MinRegionSize ||
			rec.regionSize > memorySize-out.nextBaseAddr {
			return catalogState{}, ErrInvalidCatalog
		}
		if _, exists := out.tableByName(rec.name); exists {
			return catalogState{}, ErrInvalidCatalog
		}
		out.tables = append(out.tables, rec)
		out.nextTableID++
		out.nextBaseAddr += rec.regionSize
	}
	if binary.BigEndian.Uint64(raw[catalogNextTableIDOffset:]) != out.nextTableID ||
		binary.BigEndian.Uint64(raw[catalogNextBaseAddrOffset:]) != out.nextBaseAddr {
		return catalogState{}, ErrInvalidCatalog
	}
	return out, nil
}

func encodeCatalog(c catalogState) []byte {
	buf := make([]byte, catalogSize)
	binary.BigEndian.PutUint32(buf, catalogMagic)
	binary.BigEndian.PutUint32(buf[catalogVersionOffset:], catalogFormatVersion)
	binary.BigEndian.PutUint64(buf[catalogNextTableIDOffset:], c.nextTableID)
	binary.BigEndian.PutUint64(buf[catalogNextBaseAddrOffset:], c.nextBaseAddr)
	binary.BigEndian.PutUint32(buf[catalogTableCountOffset:], uint32(len(c.tables)))
	for i, rec := range c.tables {
		off := catalogHeaderSize + i*catalogRecordSize
		r := buf[off : off+catalogRecordSize]
		binary.BigEndian.PutUint64(r, rec.tableID)
		binary.BigEndian.PutUint64(r[catalogRecordBaseAddrOffset:], rec.indexBaseAddr)
		binary.BigEndian.PutUint64(r[catalogRecordRegionSizeOffset:], rec.regionSize)
		copy(r[catalogRecordNameOffset:], rec.name)
	}
	return buf
}

func (c catalogState) tableByName(name string) (catalogTableRecord, bool) {
	for _, rec := range c.tables {
		if rec.name == name {
			return rec, true
		}
	}
	return catalogTableRecord{}, false
}

func (c catalogState) newTableRecord(name string, regionSize uint64) catalogTableRecord {
	return catalogTableRecord{
		tableID:       c.nextTableID,
		name:          name,
		indexBaseAddr: c.nextBaseAddr,
		regionSize:    regionSize,
	}
}

func (c catalogState) cloneWithTable(rec catalogTableRecord) catalogState {
	next := catalogState{
		nextTableID:  rec.tableID + 1,
		nextBaseAddr: rec.indexBaseAddr + rec.regionSize,
		tables:       make([]catalogTableRecord, 0, len(c.tables)+1),
	}
	next.tables = append(next.tables, c.tables...)
	next.tables = append(next.tables, rec)
	return next
}
