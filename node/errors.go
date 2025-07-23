package node

import "errors"

var ErrNotLeader = errors.New("not the leader")
var ErrMissingLeader = errors.New("no leader")
var ErrTimeOut = errors.New("time out")
