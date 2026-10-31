package client

import (
	"errors"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

var ErrTransactionAborted = errors.New("sinfonia: transaction aborted")

var ErrUnknownMemNode = errors.New("unknown mem node")

var ErrUnknownOutcome = minitxn.ErrUnknownOutcome
