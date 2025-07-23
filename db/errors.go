package db

import "errors"

var ErrNotFound = errors.New("not found")
var ErrAlreadyExists = errors.New("already exists")
var ErrSkippedIndex = errors.New("skipped index")
