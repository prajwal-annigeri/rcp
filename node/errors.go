package node

import "errors"

var ErrNotLeader = errors.New("not the leader")
var ErrTimeOut = errors.New("time out")
var ErrKeyRequired = errors.New("key required")
var ErrValueRequired = errors.New("value required")
