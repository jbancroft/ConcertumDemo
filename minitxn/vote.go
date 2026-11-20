package minitxn

type Vote int32

// Ordered by wire value; keep these numeric values stable (VoteOk == 0).
const (
	VoteOk Vote = iota
	VoteBadLock
	VoteBadForced
	VoteBadCmp
	VoteBadStaleEpoch
	VoteBadInternal
)

const VoteUnknown Vote = -1

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
