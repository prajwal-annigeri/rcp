package constants

const (
	// HeartbeatInterval = 100 * time.Second
	DefaultBucket = "store"

	ConsensusTimeoutMilliseconds = 1000
	ElectionTimeoutMilliseconds  = 1000
	BatchTimeoutMilliseconds     = 10
	HeartbeatIntervalMillisecond = 50

	FailureRetryCount = 3
)

var (
	LogsBucket = []byte("logs")
	KvBucket   = []byte("store")
	Usertable  = []byte("usertable")
)
