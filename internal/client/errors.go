package client

import (
	"errors"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

var ErrUnknownMemNode = errors.New("unknown mem node")

var ErrNoAdminEndpoint = errors.New("no admin endpoint configured for mem node")

var ErrUnknownOutcome = minitxn.ErrUnknownOutcome

var ErrTransactionAborted = errors.New("sinfonia: transaction aborted")
