package minitxn

import "errors"

var ErrUnknownOutcome = errors.New("sinfonia: transaction outcome unknown; operation may have committed")

func (r ExecResult) OutcomeError() error {
	if r.UnknownOutcome || r.Status == StatusUnknown {
		return errors.Join(ErrUnknownOutcome, r.Err)
	}
	return r.Err
}
