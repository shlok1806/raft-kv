package kvserver

// Op is the command that gets submitted to Raft and applied to the KV store.
type Op struct {
	Type      string // "Get" | "Put" | "Delete"
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

// GetArgs / GetReply

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Value string
	Err   Err
}

// PutArgs / PutReply

type PutArgs struct {
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type PutReply struct {
	Err Err
}

// DeleteArgs / DeleteReply

type DeleteArgs struct {
	Key       string
	ClientId  int64
	RequestId int64
}

type DeleteReply struct {
	Err Err
}

type Err string

const (
	ErrOK       Err = "OK"
	ErrNoKey    Err = "ErrNoKey"
	ErrNotLeader Err = "ErrNotLeader"
	ErrTimeout  Err = "ErrTimeout"
)
