package client

import "errors"

var ErrUnknownMemNode = errors.New("unknown mem node")

var ErrNoAdminEndpoint = errors.New("no admin endpoint configured for mem node")
