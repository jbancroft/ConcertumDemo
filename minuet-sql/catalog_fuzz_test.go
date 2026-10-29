package minuetsql

import (
	"encoding/binary"
	"testing"
)

func FuzzDecodeCatalogNeverPanics(f *testing.F) {
	valid := encodeCatalog(catalogState{
		nextTableID:  2,
		nextBaseAddr: defaultCatalogNextBase + defaultTableRegionSize,
		tables: []catalogTableRecord{{
			tableID:       1,
			name:          "accounts",
			indexBaseAddr: defaultCatalogNextBase,
			regionSize:    defaultTableRegionSize,
		}},
	})
	oversizedCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(oversizedCount[catalogTableCountOffset:], ^uint32(0))
	f.Add([]byte{}, uint64(0))
	f.Add(valid, uint64(testMemorySize))
	f.Add(oversizedCount, ^uint64(0))
	f.Fuzz(func(t *testing.T, raw []byte, memorySize uint64) {
		catalog, err := decodeCatalog(raw, memorySize)
		if err != nil {
			return
		}
		if len(catalog.tables) > catalogMaxRecords {
			t.Fatalf("decoded %d tables from a catalog with capacity %d", len(catalog.tables), catalogMaxRecords)
		}
		end := uint64(defaultCatalogNextBase)
		for _, rec := range catalog.tables {
			if rec.indexBaseAddr < end || rec.indexBaseAddr > memorySize || rec.regionSize > memorySize-rec.indexBaseAddr {
				t.Fatalf("accepted overlapping or out-of-bounds region: %+v", rec)
			}
			end = rec.indexBaseAddr + rec.regionSize
		}
		if catalog.nextBaseAddr != end || end > memorySize {
			t.Fatal("next table allocation is outside available memory")
		}
	})
}
