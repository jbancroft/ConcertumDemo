package minuetsql

import (
	"encoding/binary"
)

const (
	catalogAddr                 = 0
	catalogSize                 = 512
	catalogMagic                = uint32(0x4d53514c) // MSQL
	catalogFormatVersion        = uint32(1)
	catalogHeaderSize           = 32
	catalogRecordSize           = 80
	catalogMaxRecords           = (catalogSize - catalogHeaderSize) / catalogRecordSize
	catalogMaxNameLen           = catalogRecordSize - catalogRecordNameOffset
	catalogSchemaVersionCurrent = uint32(1)
	minuetIndexSuperblockSize   = 64
	minuetIndexRegionSize       = 1024
	defaultCatalogNextBase      = 1024
)

// Byte offsets in the catalog header and table records.
const (
	catalogVersionOffset        = 4
	catalogNextTableIDOffset    = 8
	catalogNextBaseAddrOffset   = 16
	catalogTableCountOffset     = 24
	catalogRecordBaseAddrOffset = 8
	catalogRecordSchemaOffset   = 16
	catalogRecordActiveOffset   = 20
	catalogRecordNameLenOffset  = 21
	catalogRecordNameOffset     = 24
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
	schemaVersion uint32
}

func decodeCatalog(raw []byte, cfgNextBase uint64) catalogState {
	if len(raw) < catalogSize || binary.BigEndian.Uint32(raw) != catalogMagic {
		base := cfgNextBase
		if base < defaultCatalogNextBase {
			base = defaultCatalogNextBase
		}
		if base < catalogSize+minuetIndexSuperblockSize {
			base = catalogSize + minuetIndexSuperblockSize
		}
		return catalogState{nextTableID: 1, nextBaseAddr: alignUp(base, minuetIndexRegionSize)}
	}
	tableCount := int(binary.BigEndian.Uint32(raw[catalogTableCountOffset:]))
	if tableCount > catalogMaxRecords {
		tableCount = catalogMaxRecords
	}
	out := catalogState{
		nextTableID:  binary.BigEndian.Uint64(raw[catalogNextTableIDOffset:]),
		nextBaseAddr: binary.BigEndian.Uint64(raw[catalogNextBaseAddrOffset:]),
		tables:       make([]catalogTableRecord, 0, tableCount),
	}
	if out.nextTableID == 0 {
		out.nextTableID = 1
	}
	if out.nextBaseAddr < defaultCatalogNextBase {
		out.nextBaseAddr = defaultCatalogNextBase
	}
	if out.nextBaseAddr < catalogSize+minuetIndexSuperblockSize {
		out.nextBaseAddr = catalogSize + minuetIndexSuperblockSize
	}
	off := catalogHeaderSize
	for i := 0; i < tableCount && off+catalogRecordSize <= len(raw); i++ {
		recRaw := raw[off : off+catalogRecordSize]
		off += catalogRecordSize
		if recRaw[catalogRecordActiveOffset] != 1 {
			continue
		}
		nameLen := int(recRaw[catalogRecordNameLenOffset])
		if nameLen > catalogMaxNameLen {
			nameLen = catalogMaxNameLen
		}
		name := normalizeName(string(recRaw[catalogRecordNameOffset : catalogRecordNameOffset+nameLen]))
		if name == "" {
			continue
		}
		out.tables = append(out.tables, catalogTableRecord{
			tableID:       binary.BigEndian.Uint64(recRaw),
			indexBaseAddr: binary.BigEndian.Uint64(recRaw[catalogRecordBaseAddrOffset:]),
			schemaVersion: binary.BigEndian.Uint32(recRaw[catalogRecordSchemaOffset:]),
			name:          name,
		})
	}
	return out
}

func encodeCatalog(c catalogState) []byte {
	buf := make([]byte, catalogSize)
	binary.BigEndian.PutUint32(buf, catalogMagic)
	binary.BigEndian.PutUint32(buf[catalogVersionOffset:], catalogFormatVersion)
	binary.BigEndian.PutUint64(buf[catalogNextTableIDOffset:], c.nextTableID)
	binary.BigEndian.PutUint64(buf[catalogNextBaseAddrOffset:], c.nextBaseAddr)
	binary.BigEndian.PutUint32(buf[catalogTableCountOffset:], uint32(len(c.tables)))
	off := catalogHeaderSize
	for _, rec := range c.tables {
		if off+catalogRecordSize > len(buf) {
			break
		}
		r := buf[off : off+catalogRecordSize]
		off += catalogRecordSize
		binary.BigEndian.PutUint64(r, rec.tableID)
		binary.BigEndian.PutUint64(r[catalogRecordBaseAddrOffset:], rec.indexBaseAddr)
		binary.BigEndian.PutUint32(r[catalogRecordSchemaOffset:], rec.schemaVersion)
		r[catalogRecordActiveOffset] = 1
		name := []byte(rec.name)
		if len(name) > catalogMaxNameLen {
			name = name[:catalogMaxNameLen]
		}
		r[catalogRecordNameLenOffset] = byte(len(name))
		copy(r[catalogRecordNameOffset:], name)
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

func (c catalogState) newTableRecord(name string) catalogTableRecord {
	base := alignUp(c.nextBaseAddr, minuetIndexRegionSize)
	return catalogTableRecord{
		tableID:       c.nextTableID,
		name:          name,
		indexBaseAddr: base,
		schemaVersion: catalogSchemaVersionCurrent,
	}
}

func (c catalogState) cloneWithTable(rec catalogTableRecord) catalogState {
	next := catalogState{
		nextTableID:  rec.tableID + 1,
		nextBaseAddr: rec.indexBaseAddr + minuetIndexRegionSize,
		tables:       make([]catalogTableRecord, 0, len(c.tables)+1),
	}
	next.tables = append(next.tables, c.tables...)
	next.tables = append(next.tables, rec)
	return next
}

func alignUp(v, block uint64) uint64 {
	if block == 0 {
		return v
	}
	rem := v % block
	if rem == 0 {
		return v
	}
	return v + block - rem
}
