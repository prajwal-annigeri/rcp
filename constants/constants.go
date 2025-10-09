package constants

const (
	// HeartbeatInterval = 100 * time.Second
	DefaultBucket = "store"

	FailureRetryCount       = 3
	MaxInFlightMessageCount = 100
)

var (
	LogsBucket = []byte("logs")
	KvBucket   = []byte("store")
	Usertable  = []byte("usertable")
)
