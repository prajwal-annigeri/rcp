package db

import "errors"

var ErrNotFound = errors.New("not found")
var ErrSkippedIndex = errors.New("skipped index")
