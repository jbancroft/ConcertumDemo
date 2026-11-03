package sinfoniaclient_test

import (
	"reflect"
	"strings"
	"testing"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

func TestPublicAPISignaturesDoNotExposeInternalPackages(t *testing.T) {
	checkNoInternalType(t, reflect.TypeOf(sinfoniaclient.NewClient))
	checkNoInternalType(t, reflect.TypeFor[*sinfoniaclient.Client]())
	checkNoInternalType(t, reflect.TypeFor[*sinfoniaclient.MiniTransaction]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.ExecResult]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.ReadKey]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.CompareItem]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.ReadItem]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.WriteItem]())
	checkNoInternalType(t, reflect.TypeFor[sinfoniaclient.TxnClient]())
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
			for field := range cur.Fields() {
				if field.IsExported() {
					walk(field.Type)
				}
			}
		case reflect.Interface:
			for method := range cur.Methods() {
				walk(method.Type)
			}
		case reflect.Func:
			for in := range cur.Ins() {
				walk(in)
			}
			for out := range cur.Outs() {
				walk(out)
			}
		}
		for method := range cur.Methods() {
			walk(method.Type)
		}
	}
	walk(typ)
}
