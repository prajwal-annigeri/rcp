package node

import "errors"

var ErrNotLeader = errors.New("not the leader")
var ErrMissingLeader = errors.New("no leader")
var ErrTimeOut = errors.New("time out")
var ErrNotAlive = errors.New("not alive")
var ErrTooManyInFlightMessages = errors.New("too many in flight messages")
var ErrInvalidReconfiguration = errors.New("invalid reconfiguration")
