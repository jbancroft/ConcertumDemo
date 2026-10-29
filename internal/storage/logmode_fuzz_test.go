package storage

import "testing"

func FuzzParseTIDKeyNeverPanics(f *testing.F) {
	f.Add("1:2:3")
	f.Add("18446744073709551615:18446744073709551615:18446744073709551615")
	f.Add("1:2")
	f.Add("-1:2:3")
	f.Add("1:2:3:4")
	f.Fuzz(func(t *testing.T, key string) {
		txId, ok := parseTidKey(key)
		if !ok {
			return
		}
		canonical := tidKey(txId)
		reparsed, ok := parseTidKey(canonical)
		if !ok || reparsed != txId {
			t.Fatalf("canonical key %q did not preserve transaction ID %+v", canonical, txId)
		}
	})
}
