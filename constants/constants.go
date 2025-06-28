package constants

const (
	// HeartbeatInterval = 100 * time.Second
	MaxLogsPerAppendEntry = 1000
	DefaultBucket = "store"
	
)

var (
	LogsBucket      = []byte("logs")
	KvBucket        = []byte("store")
	Usertable       = []byte("usertable")
)

