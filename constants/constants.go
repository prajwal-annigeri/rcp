package constants

const (
	// HeartbeatInterval = 100 * time.Second
	MaxLogsPerAppendEntry = 100
	DefaultBucket = "store"
	
)

var (
	LogsBucket      = []byte("logs")
	KvBucket        = []byte("store")
	SavingsBucket   = []byte("savings")
	CheckingBucket  = []byte("checking")
	LocksBucket     = []byte("locks")
	PendingSavings  = []byte("pending_savings")
	PendingChecking = []byte("pending_checking")
	Usertable       = []byte("usertable")
)

