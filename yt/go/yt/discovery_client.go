package yt

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	zaplog "go.ytsaurus.tech/library/go/core/log/zap"
)

type (
	ListMembersOptions struct {
		Limit         *int32
		AttributeKeys []string
	}

	Attribute struct {
		Key   string
		Value []byte
	}

	MemberInfo struct {
		ID         string
		Priority   int64
		Revision   int64
		Attributes []*Attribute
	}

	GetGroupMetaOptions struct {
	}

	GroupMeta struct {
		MemberCount int32
	}

	HeartbeatOptions struct {
	}
)

type DiscoveryClient interface {
	// http:verb:"list_members"
	// http:params:"group_id"
	ListMembers(
		ctx context.Context,
		groupID string,
		options *ListMembersOptions,
	) (members []*MemberInfo, err error)

	// http:verb:"get_group_meta"
	// http:params:"group_id"
	GetGroupMeta(
		ctx context.Context,
		groupID string,
		options *GetGroupMetaOptions,
	) (meta *GroupMeta, err error)

	// http:verb:"heartbeat"
	// http:params:"group_id","member_info","lease_timeout"
	Heartbeat(
		ctx context.Context,
		groupID string,
		memberInfo MemberInfo,
		leaseTimeout int64,
		opts *HeartbeatOptions,
	) (err error)

	// Stop() cancels and waits for completion of all background activity associated with this client.
	Stop()
}

const (
	DefaultRequestTimeout = 5 * time.Minute
)

type DiscoveryConfig struct {
	// DiscoveryServers is list of servers on which discovery client can send requests.
	DiscoveryServers []string

	// EndpointSet is id of endpoint set of discovery servers.
	//
	// If this field is set, then YPClusters must be set too.
	EndpointSet string

	// YPClusters is list of yp clusters where discovery servers are located.
	YPClusters []string

	// Token configures OAuth token used by the client.
	//
	// If Token is not set, value of YT_TOKEN environment variable is used instead.
	Token string

	// RequestTimeout specifies default timeout for requests. Timeout includes all retries and backoffs.
	// Timeout for single request is not configurable right now.
	//
	// A Timeout of zero means no timeout. Client can still specify timeout on per-request basis using context.
	//
	// nil value means default timeout of 5 minutes.
	RequestTimeout *time.Duration

	// Logger overrides default logger, used by the client.
	//
	// When Logger is not set, logging behaviour is configured by YT_LOG_LEVEL environment variable.
	//
	// If YT_LOG_LEVEL is not set, no logging is performed. Otherwise logs are written to stderr,
	// with log level derived from value of YT_LOG_LEVEL variable.
	//
	// WARNING: Running YT client in production without debug logs is highly discouraged.
	Logger log.Structured

	// AllowRequestsFromJob explicitly allows creating client inside YT job.
	//
	// WARNING: This option can be enabled ONLY after explicit approval from YT team. If you enable this option
	// without approval, your might be BANNED.
	//
	// If you need to read tables, or access cypress from YT job, use API provided by mapreduce package, or
	// redesign your application.
	//
	// Typical mapreduce operation can launch hundred of thousands concurrent jobs. If each job makes even a single request,
	// that could easily lead to master/proxy overload.
	AllowRequestsFromJob bool
}

func (c *DiscoveryConfig) GetToken() string {
	if c.Token != "" {
		return c.Token
	}

	return os.Getenv("YT_TOKEN")
}

func (c *DiscoveryConfig) GetLogger() log.Structured {
	if c.Logger != nil {
		return c.Logger
	}

	logLevel := os.Getenv("YT_LOG_LEVEL")
	if logLevel == "" {
		return (&nop.Logger{}).Structured()
	}

	lvl, err := log.ParseLevel(logLevel)
	if err != nil {
		lvl = log.DebugLevel
	}

	config := zaplog.ConsoleConfig(lvl)
	config.OutputPaths = []string{"stderr"}

	l, err := zaplog.New(config)
	if err != nil {
		panic(fmt.Sprintf("failed to configure default logger: %+v", err))
	}
	return l.Structured()
}

func (c *DiscoveryConfig) GetRequestTimeout() time.Duration {
	if c.RequestTimeout != nil {
		return *c.RequestTimeout
	}

	return DefaultRequestTimeout
}

type Discoverer interface {
	ListDiscoveryServers() ([]string, error)
	Stop()
}

type staticDiscoverer struct {
	discoveryServers []string
}

func NewStaticDiscoverer(conf *DiscoveryConfig) *staticDiscoverer {
	return &staticDiscoverer{conf.DiscoveryServers}
}

func (d *staticDiscoverer) ListDiscoveryServers() ([]string, error) {
	return d.discoveryServers, nil
}

func (d *staticDiscoverer) Stop() {}
