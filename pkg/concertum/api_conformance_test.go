package concertum_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jbancroft/concertum2/pkg/concertum"
)

func TestPublicAPISignaturesDoNotExposeInternalPackages(t *testing.T) {
	checkNoInternalType(t, reflect.TypeOf(concertum.NewClient))
	checkNoInternalType(t, reflect.TypeOf(concertum.NewDirectoryBackedClient))
	checkNoInternalType(t, reflect.TypeOf(&concertum.Client{}))
	checkNoInternalType(t, reflect.TypeOf(&concertum.MTx{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.Stats{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.ExecResult{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.ReadKey{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.CmpItem{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.ReadItem{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.WriteItem{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.RateWindows{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.TxnRateGroup{}))
	checkNoInternalType(t, reflect.TypeOf(concertum.TxnClassStats{}))
}

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
				walk(cur.Field(i).Type)
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
