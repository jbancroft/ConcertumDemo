package client

import (
	"testing"
	"time"

	"github.com/jbancroft/concertum2/internal/proto"
)

func TestComparisonsValidAt(t *testing.T) {
	now := time.Date(2026, time.October, 26, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		deadlines []time.Time
		want      bool
	}{
		{name: "no comparisons", want: true},
		{name: "no deadline", deadlines: []time.Time{{}}, want: true},
		{name: "before expiry", deadlines: []time.Time{now.Add(time.Nanosecond)}, want: true},
		{name: "exactly at expiry", deadlines: []time.Time{now}, want: false},
		{name: "after expiry", deadlines: []time.Time{now.Add(-time.Nanosecond)}, want: false},
		{name: "all unexpired", deadlines: []time.Time{{}, now.Add(time.Second), now.Add(time.Hour)}, want: true},
		{name: "any expired comparison aborts", deadlines: []time.Time{now.Add(time.Second), {}, now.Add(-time.Second)}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var items []proto.CmpItem
			for _, deadline := range test.deadlines {
				items = append(items, proto.CmpItem{ValidUntil: deadline})
			}
			if got := comparisonsValidAt(items, now); got != test.want {
				t.Fatalf("comparisonsValidAt()=%t, want %t", got, test.want)
			}
		})
	}
}
