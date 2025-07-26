package constants

const (
	// HeartbeatInterval = 100 * time.Second
	DefaultBucket = "store"

	FailureRetryCount = 3
)

var (
	LogsBucket = []byte("logs")
	KvBucket   = []byte("store")
	Usertable  = []byte("usertable")
)
