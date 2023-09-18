package discoveryclient

import "go.ytsaurus.tech/yt/go/yt/internal/rpcclient"

const (
	MethodListMembers  rpcclient.Method = "ListMembers"
	MethodGetGroupMeta rpcclient.Method = "GetGroupMeta"
	MethodHeartbeat    rpcclient.Method = "Heartbeat"
)
