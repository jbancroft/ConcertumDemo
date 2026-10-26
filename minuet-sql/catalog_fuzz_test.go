package minuetsql

import (
	"encoding/binary"
	"testing"
)

func FuzzDecodeCatalogNeverPanics(f *testing.F) {
	valid := encodeCatalog(catalogState{
		nextTableID:  2,
		nextBaseAddr: defaultCatalogNextBase,
		tables: []catalogTableRecord{{
			tableID:       1,
			name:          "accounts",
			indexBaseAddr: defaultCatalogNextBase,
			schemaVersion: catalogSchemaVersionCurrent,
		}},
	})
	oversizedCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(oversizedCount[24:28], ^uint32(0))
	f.Add([]byte{}, uint64(0))
	f.Add(valid, uint64(defaultCatalogNextBase))
	f.Add(oversizedCount, ^uint64(0))
	f.Fuzz(func(t *testing.T, raw []byte, nextBase uint64) {
		catalog := decodeCatalog(raw, nextBase)
		if len(catalog.tables) > catalogMaxRecords {
			t.Fatalf("decoded %d tables from a catalog with capacity %d", len(catalog.tables), catalogMaxRecords)
		}
	})
}
