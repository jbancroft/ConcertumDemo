package minitxn

// Vote is a participant's answer to a prepare request.
// Keep the numeric values stable: existing wire messages use VoteOk == 0.
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

// String returns the vote names used in gofonia.
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
