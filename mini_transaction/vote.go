package mini_transaction

// Vote represents a participant's prepare-phase outcome. The existing
// Concertum2 wire values are retained: VoteOk is zero, unlike gofonia's enum.
type Vote int32

const (
	VoteOk Vote = iota
	VoteBadLock
	VoteBadForced
	VoteBadCmp
	VoteBadStaleEpoch
	VoteBadInternal
)

const VoteUnknown Vote = -1

// String follows gofonia's diagnostic vote names.
func (v Vote) String() string {
	switch v {
	case VoteOk:
		return "VoteOk"
	case VoteBadLock:
		return "VoteBadLock"
	case VoteBadForced:
		return "VoteBadForced"
	case VoteBadCmp:
		return "VoteBadCmp"
	case VoteBadStaleEpoch:
		return "VoteBadStaleEpoch"
	case VoteBadInternal:
		return "VoteBadInternal"
	case VoteUnknown:
		return "VoteUnknown"
	default:
		return "UnknownVote"
	}
}
