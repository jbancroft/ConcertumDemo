package sinfoniaclient_test

import (
	"reflect"
	"strings"
	"testing"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

func TestPublicAPISignaturesDoNotExposeInternalPackages(t *testing.T) {
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.NewClient))
	checkNoInternalType(t, reflect.TypeOf(&sinfoniaclient.Client{}))
	checkNoInternalType(t, reflect.TypeOf(&sinfoniaclient.MiniTransaction{}))
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.ExecResult{}))
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.ReadKey{}))
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.CompareItem{}))
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.ReadItem{}))
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.WriteItem{}))
	checkNoInternalType(t, reflect.TypeOf((*sinfoniaclient.TxnClient)(nil)).Elem())
}

var _ sinfoniaclient.TxnClient = (*sinfoniaclient.Client)(nil)

func checkNoInternalType(t *testing.T, typ reflect.Type) {
	t.Helper()
	seen := map[reflect.Type]struct{}{}
	var walk func(reflect.Type)
	walk = func(cur reflect.Type) {
		if cur == nil {
			return
		}
		if _, ok := seen[cur]; ok {
			return
		}
		seen[cur] = struct{}{}
		if strings.Contains(cur.PkgPath(), "/internal/") {
			t.Fatalf("public API leaked internal type: %s (pkg=%s)", cur.String(), cur.PkgPath())
		}
		switch cur.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			walk(cur.Elem())
		case reflect.Map:
			walk(cur.Key())
			walk(cur.Elem())
		case reflect.Struct:
			for i := 0; i < cur.NumField(); i++ {
				if field := cur.Field(i); field.IsExported() {
					walk(field.Type)
				}
			}
		case reflect.Interface:
			for i := 0; i < cur.NumMethod(); i++ {
				walk(cur.Method(i).Type)
			}
		case reflect.Func:
			for i := 0; i < cur.NumIn(); i++ {
				walk(cur.In(i))
			}
			for i := 0; i < cur.NumOut(); i++ {
				walk(cur.Out(i))
			}
		}
		for i := 0; i < cur.NumMethod(); i++ {
			walk(cur.Method(i).Type)
		}
	}
	walk(typ)
}
