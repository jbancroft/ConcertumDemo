package client

import "errors"

var ErrUnknownMemNode = errors.New("unknown mem node")

var ErrNoAdminEndpoint = errors.New("no admin endpoint configured for mem node")

var ErrUnknownOutcome = errors.New("sinfonia: transaction outcome unknown; operation may have committed")

var ErrTransactionAborted = errors.New("sinfonia: transaction aborted")
