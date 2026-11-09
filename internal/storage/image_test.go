package storage

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

type imageFileStub struct {
	data      []byte
	synced    []byte
	syncCalls int
	syncErr   error
	writeErr  error
	short     bool
}

func (f *imageFileStub) WriteAt(data []byte, offset int64) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.short {
		return 0, nil
	}
	return copy(f.data[offset:], data), nil
}

func (f *imageFileStub) Sync() error {
	f.syncCalls++
	f.synced = bytes.Clone(f.data)
	return f.syncErr
}

func (f *imageFileStub) Close() error { return nil }

func TestDiskImageSyncsCompleteTransaction(t *testing.T) {
	t.Parallel()
	f := &imageFileStub{data: make([]byte, 8)}
	d := &DiskImage{file: f, size: len(f.data)}
	writes := []sinfonia.WriteItem{
		{Addr: 0, Data: []byte("abcd")},
		{Addr: 2, Data: []byte("XY")},
		{Addr: 7, Data: []byte("z")},
	}
	if err := d.ApplyWrites(writes); err != nil {
		t.Fatal(err)
	}
	if f.syncCalls != 1 || string(f.synced) != "abXY\x00\x00\x00z" {
		t.Fatalf("syncs=%d durable bytes=%q", f.syncCalls, f.synced)
	}
	if err := d.ApplyWrites(nil); err != nil || f.syncCalls != 1 {
		t.Fatalf("empty write set: syncs=%d err=%v", f.syncCalls, err)
	}
}

func TestDiskImageRejectsInvalidWriteSetBeforeChangingBytes(t *testing.T) {
	t.Parallel()
	for _, addr := range []uint64{8, math.MaxUint64} {
		f := &imageFileStub{data: make([]byte, 8)}
		d := &DiskImage{file: f, size: len(f.data)}
		writes := []sinfonia.WriteItem{{Data: []byte("first")}, {Addr: addr, Data: []byte("bad")}}
		if err := d.ApplyWrites(writes); err == nil {
			t.Fatalf("accepted invalid address %d", addr)
		}
		if !bytes.Equal(f.data, make([]byte, 8)) || f.syncCalls != 0 {
			t.Fatalf("invalid write set changed bytes: %q, syncs=%d", f.data, f.syncCalls)
		}
	}
}

func TestDiskImagePropagatesWriteAndSyncFailures(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected image failure")
	for _, tc := range []struct {
		name      string
		file      imageFileStub
		want      error
		wantSyncs int
	}{
		{name: "write", file: imageFileStub{writeErr: injected}, want: injected},
		{name: "short write", file: imageFileStub{short: true}, want: io.ErrShortWrite},
		{name: "sync", file: imageFileStub{syncErr: injected}, want: injected, wantSyncs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.file
			f.data = make([]byte, 8)
			d := &DiskImage{file: &f, size: len(f.data)}
			err := d.ApplyWrites([]sinfonia.WriteItem{{Data: []byte("a")}, {Addr: 4, Data: []byte("b")}})
			if !errors.Is(err, tc.want) || f.syncCalls != tc.wantSyncs {
				t.Fatalf("err=%v syncs=%d, want %v and %d", err, f.syncCalls, tc.want, tc.wantSyncs)
			}
		})
	}
}

func TestDiskImageWriteSetSurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	d, _, err := OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.ApplyWrites([]sinfonia.WriteItem{{Data: []byte("ab")}, {Addr: 6, Data: []byte("yz")}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, data, err := OpenDiskImage(dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if string(data) != "ab\x00\x00\x00\x00yz" {
		t.Fatalf("reopened bytes=%q", data)
	}
}
