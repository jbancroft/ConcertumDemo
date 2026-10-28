package minitxn

import "testing"

func TestVoteNamesAndWireValues(t *testing.T) {
	for _, test := range []struct {
		vote  Vote
		value int32
		name  string
	}{
		{VoteOk, 0, "VoteOk"},
		{VoteBadLock, 1, "VoteBadLock"},
		{VoteBadForced, 2, "VoteBadForced"},
		{VoteBadCmp, 3, "VoteBadCmp"},
		{VoteBadStaleEpoch, 4, "VoteBadStaleEpoch"},
		{VoteBadInternal, 5, "VoteBadInternal"},
		{VoteUnknown, -1, "VoteUnknown"},
		{Vote(99), 99, "UnknownVote"},
	} {
		if int32(test.vote) != test.value || test.vote.String() != test.name {
			t.Fatalf("vote=%d (%s), want %d (%s)", test.vote, test.vote, test.value, test.name)
		}
	}
}
