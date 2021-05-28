package rpcclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/library/go/certifi"
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/bus"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	"a.yandex-team.ru/yt/go/proto/core/ytree"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal"
	"a.yandex-team.ru/yt/go/yterrors"
)

type cypressClient struct {
	conf       *yt.Config
	clusterURL yt.ClusterURL
	token      string

	log log.Structured

	// httpClient is used to retrieve available proxies.
	httpClient *http.Client
	proxySet   *internal.ProxySet

	connPool ConnPool
	stop     *internal.StopGroup
}

func NewCypressClient(conf *yt.Config) (*cypressClient, error) {
	c := &cypressClient{
		conf:       conf,
		clusterURL: yt.NormalizeProxyURL(conf.Proxy),
		log:        conf.GetLogger(),
		stop:       internal.NewStopGroup(),
	}

	certPool, err := certifi.NewCertPool()
	if err != nil {
		return nil, err
	}

	c.httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        0,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,

			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: certPool,
			},
		},
		Timeout: 60 * time.Second,
	}

	if token := conf.GetToken(); token != "" {
		c.token = token
	}

	c.proxySet = &internal.ProxySet{UpdateFn: c.listRPCProxies}

	c.connPool = NewLRUConnPool(func(ctx context.Context, addr string) (*bus.ClientConn, error) {
		clientOpts := []bus.ClientOption{
			bus.WithLogger(c.log.Logger()),
			bus.WithDefaultProtocolVersionMajor(1),
		}
		return bus.NewClient(ctx, addr, clientOpts...)
	}, connPoolSize)

	return c, nil
}

func (c *cypressClient) schema() string {
	schema := "http"
	if c.conf.UseTLS {
		schema = "https"
	}
	return schema
}

func (c *cypressClient) listRPCProxies() ([]string, error) {
	if !c.stop.TryAdd() {
		return nil, xerrors.New("client is stopped")
	}
	defer c.stop.Done()

	v := url.Values{"type": {"rpc"}}
	if c.conf.ProxyRole != "" {
		v.Add("role", c.conf.ProxyRole)
	}

	var resolveURL url.URL
	resolveURL.Scheme = c.schema()
	resolveURL.Host = c.clusterURL.Address
	resolveURL.Path = "api/v4/discover_proxies"
	resolveURL.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", resolveURL.String(), nil)
	if err != nil {
		return nil, err
	}

	var rsp *http.Response
	rsp, err = c.httpClient.Do(req.WithContext(c.stop.Context()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rsp.Body.Close() }()

	if rsp.StatusCode != http.StatusOK {
		return nil, unexpectedStatusCode(rsp)
	}

	var proxies struct {
		Proxies []string `json:"proxies"`
	}
	if err = json.NewDecoder(rsp.Body).Decode(&proxies); err != nil {
		return nil, err
	}

	if len(proxies.Proxies) == 0 {
		return nil, xerrors.New("rpc proxy list is empty")
	}

	return proxies.Proxies, nil
}

func (c *cypressClient) do(
	ctx context.Context,
	method string,
	request proto.Message,
	reply proto.Message,
	opts ...bus.SendOption,
) error {
	addr, err := c.proxySet.PickRandom(ctx)
	if err != nil {
		return err
	}

	conn, err := c.connPool.Conn(ctx, addr)
	if err != nil {
		return err
	}
	c.log.Debug("got bus conn", log.String("fqdn", addr))

	err = conn.Send(ctx, "ApiService", method, request, reply, append(opts, bus.WithToken(c.token))...)
	if c.shouldBanProxy(err) {
		c.log.Debug("banning rpc proxy", log.String("fqdn", addr))
		c.proxySet.BanProxy(addr)
		c.connPool.Discard(addr)
	}

	return err
}

func (c *cypressClient) shouldBanProxy(err error) bool {
	if err == nil {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var ytErr *yterrors.Error
	if errors.As(err, &ytErr) && ytErr.Message == "Proxy is banned" {
		return true
	}

	return false
}

func (c *cypressClient) Stop() {
	_ = c.connPool.Close()
	c.stop.Stop()
}

func (c *cypressClient) CreateNode(
	ctx context.Context,
	path ypath.YPath,
	typ yt.NodeType,
	opts *yt.CreateNodeOptions,
) (id yt.NodeID, err error) {
	if opts == nil {
		opts = &yt.CreateNodeOptions{}
	}

	attrs, err := convertAttributes(opts.Attributes)
	if err != nil {
		err = xerrors.Errorf("unable to serialize attributes: %w", err)
		return
	}

	objectType, err := convertObjectType(typ)
	if err != nil {
		return
	}

	req := &rpc_proxy.TReqCreateNode{
		Path:                 ptr.String(path.YPath().String()),
		Type:                 ptr.Int32(int32(objectType)),
		Attributes:           attrs,
		Recursive:            &opts.Recursive,
		Force:                &opts.Force,
		IgnoreExisting:       &opts.IgnoreExisting,
		LockExisting:         nil, // todo check unimportant
		IgnoreTypeMismatch:   nil, // todo check unimportant
		TransactionalOptions: convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:  convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:      convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspCreateNode
	err = c.do(ctx, "CreateNode", req, &rsp)
	if err != nil {
		return
	}

	id = makeNodeID(rsp.GetNodeId())
	return
}

func (c *cypressClient) CreateObject(ctx context.Context, typ yt.NodeType, opts *yt.CreateObjectOptions) (id yt.NodeID, err error) {
	if opts == nil {
		opts = &yt.CreateObjectOptions{}
	}

	attrs, err := convertAttributes(opts.Attributes)
	if err != nil {
		err = xerrors.Errorf("unable to serialize attributes: %w", err)
		return
	}

	objectType, err := convertObjectType(typ)
	if err != nil {
		return
	}

	req := &rpc_proxy.TReqCreateObject{
		Type:           ptr.Int32(int32(objectType)),
		Attributes:     attrs,
		IgnoreExisting: &opts.IgnoreExisting,
	}

	var rsp rpc_proxy.TRspCreateObject
	err = c.do(ctx, "CreateObject", req, &rsp)
	if err != nil {
		return
	}

	id = makeNodeID(rsp.GetObjectId())
	return
}

func (c *cypressClient) NodeExists(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.NodeExistsOptions,
) (ok bool, err error) {
	if opts == nil {
		opts = &yt.NodeExistsOptions{}
	}

	req := &rpc_proxy.TReqExistsNode{
		Path:                              ptr.String(path.YPath().String()),
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:               nil, // todo
		MasterReadOptions:                 convertMasterReadOptions(opts.MasterReadOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}

	var rsp rpc_proxy.TRspExistsNode
	err = c.do(ctx, "ExistsNode", req, &rsp)
	if err != nil {
		return
	}

	ok = rsp.GetExists()
	return
}

func (c *cypressClient) RemoveNode(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.RemoveNodeOptions,
) (err error) {
	if opts == nil {
		opts = &yt.RemoveNodeOptions{}
	}

	req := &rpc_proxy.TReqRemoveNode{
		Path:                 ptr.String(path.YPath().String()),
		Recursive:            &opts.Recursive,
		Force:                &opts.Force,
		TransactionalOptions: convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:  convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:      convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspRemoveNode
	err = c.do(ctx, "RemoveNode", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *cypressClient) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result interface{},
	opts *yt.GetNodeOptions,
) (err error) {
	if opts == nil {
		opts = &yt.GetNodeOptions{}
	}

	req := &rpc_proxy.TReqGetNode{
		Path:                              ptr.String(path.YPath().String()),
		Attributes:                        convertAttributeKeys(opts.Attributes),
		MaxSize:                           opts.MaxSize,
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:               convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MasterReadOptions:                 convertMasterReadOptions(opts.MasterReadOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}

	if len(opts.Attributes) != 0 {
		req.Attributes = &rpc_proxy.TAttributeKeys{
			Columns: opts.Attributes,
		}
	}

	var rsp rpc_proxy.TRspGetNode
	err = c.do(ctx, "GetNode", req, &rsp)
	if err != nil {
		return err
	}

	if err := yson.Unmarshal(rsp.Value, result); err != nil {
		return err
	}

	return nil
}

func (c *cypressClient) SetNode(
	ctx context.Context,
	path ypath.YPath,
	value interface{},
	opts *yt.SetNodeOptions,
) (err error) {
	if opts == nil {
		opts = &yt.SetNodeOptions{}
	}

	valueBytes, err := yson.Marshal(value)
	if err != nil {
		err = xerrors.Errorf("unable to serialize value: %w", err)
		return
	}

	req := &rpc_proxy.TReqSetNode{
		Path:                              ptr.String(path.YPath().String()),
		Value:                             valueBytes,
		Recursive:                         &opts.Recursive,
		Force:                             &opts.Force,
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:               convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:                   convertMutatingOptions(opts.MutatingOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}

	var rsp rpc_proxy.TRspSetNode
	err = c.do(ctx, "SetNode", req, &rsp)
	if err != nil {
		return err
	}

	return nil
}

func (c *cypressClient) MultisetAttributes(
	ctx context.Context,
	path ypath.YPath,
	attrs map[string]interface{},
	opts *yt.MultisetAttributesOptions,
) (err error) {
	if opts == nil {
		opts = &yt.MultisetAttributesOptions{}
	}

	subrequests := make([]*rpc_proxy.TReqMultisetAttributesNode_TSubrequest, 0, len(attrs))
	for key, value := range attrs {
		valueBytes, err := yson.Marshal(value)
		if err != nil {
			return xerrors.Errorf("unable to serialize attribute %q: %w", value, err)
		}

		subrequests = append(subrequests, &rpc_proxy.TReqMultisetAttributesNode_TSubrequest{
			Attribute: ptr.String(key),
			Value:     valueBytes,
		})
	}

	req := &rpc_proxy.TReqMultisetAttributesNode{
		Path:                              ptr.String(path.YPath().String()),
		Subrequests:                       subrequests,
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:               convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:                   convertMutatingOptions(opts.MutatingOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}

	var rsp rpc_proxy.TRspMultisetAttributesNode
	err = c.do(ctx, "MultisetAttributesNode", req, &rsp)
	if err != nil {
		return err
	}

	return nil
}

func (c *cypressClient) ListNode(
	ctx context.Context,
	path ypath.YPath,
	result interface{},
	opts *yt.ListNodeOptions,
) (err error) {
	if opts == nil {
		opts = &yt.ListNodeOptions{}
	}

	req := &rpc_proxy.TReqListNode{
		Path:                              ptr.String(path.YPath().String()),
		Attributes:                        convertAttributeKeys(opts.Attributes),
		MaxSize:                           opts.MaxSize,
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:               convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MasterReadOptions:                 convertMasterReadOptions(opts.MasterReadOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}

	if len(opts.Attributes) != 0 {
		req.Attributes = &rpc_proxy.TAttributeKeys{
			Columns: opts.Attributes,
		}
	}

	var rsp rpc_proxy.TRspListNode
	err = c.do(ctx, "ListNode", req, &rsp)
	if err != nil {
		return err
	}

	if err := yson.Unmarshal(rsp.Value, result); err != nil {
		return err
	}

	return nil
}

func (c *cypressClient) CopyNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	opts *yt.CopyNodeOptions,
) (id yt.NodeID, err error) {
	if opts == nil {
		opts = &yt.CopyNodeOptions{}
	}

	req := &rpc_proxy.TReqCopyNode{
		SrcPath:                   ptr.String(src.YPath().String()),
		DstPath:                   ptr.String(dst.YPath().String()),
		Recursive:                 &opts.Recursive,
		Force:                     &opts.Force,
		PreserveAccount:           opts.PreserveAccount,
		PreserveCreationTime:      opts.PreserveCreationTime,
		PreserveModificationTime:  nil, // todo
		PreserveExpirationTime:    opts.PreserveExpirationTime,
		PreserveExpirationTimeout: opts.PreserveExpirationTimeout,
		PreserveOwner:             nil, // todo
		PreserveAcl:               nil, // todo
		IgnoreExisting:            &opts.IgnoreExisting,
		LockExisting:              nil, // todo
		PessimisticQuotaCheck:     opts.PessimisticQuotaCheck,
		TransactionalOptions:      convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:       convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:           convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspCopyNode
	err = c.do(ctx, "CopyNode", req, &rsp)
	if err != nil {
		return
	}

	id = makeNodeID(rsp.GetNodeId())
	return
}

func (c *cypressClient) MoveNode(
	ctx context.Context,
	src ypath.YPath,
	dst ypath.YPath,
	opts *yt.MoveNodeOptions,
) (id yt.NodeID, err error) {
	if opts == nil {
		opts = &yt.MoveNodeOptions{}
	}

	req := &rpc_proxy.TReqMoveNode{
		SrcPath:                   ptr.String(src.YPath().String()),
		DstPath:                   ptr.String(dst.YPath().String()),
		Recursive:                 &opts.Recursive,
		Force:                     &opts.Force,
		PreserveAccount:           opts.PreserveAccount,
		PreserveCreationTime:      nil, // todo
		PreserveModificationTime:  nil, // todo
		PreserveExpirationTime:    opts.PreserveExpirationTime,
		PreserveExpirationTimeout: opts.PreserveExpirationTimeout,
		PreserveOwner:             nil, // todo
		PessimisticQuotaCheck:     opts.PessimisticQuotaCheck,
		TransactionalOptions:      convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:       convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:           convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspMoveNode
	err = c.do(ctx, "MoveNode", req, &rsp)
	if err != nil {
		return
	}

	id = makeNodeID(rsp.GetNodeId())
	return
}

func (c *cypressClient) LinkNode(
	ctx context.Context,
	target ypath.YPath,
	link ypath.YPath,
	opts *yt.LinkNodeOptions,
) (id yt.NodeID, err error) {
	if opts == nil {
		opts = &yt.LinkNodeOptions{}
	}

	req := &rpc_proxy.TReqLinkNode{
		SrcPath:              ptr.String(target.YPath().String()),
		DstPath:              ptr.String(link.YPath().String()),
		Recursive:            &opts.Recursive,
		Force:                &opts.Force,
		IgnoreExisting:       &opts.IgnoreExisting,
		LockExisting:         nil, // todo
		TransactionalOptions: convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:  convertPrerequisiteOptions(opts.PrerequisiteOptions),
		MutatingOptions:      convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspMoveNode
	err = c.do(ctx, "LinkNode", req, &rsp)
	if err != nil {
		return
	}

	id = makeNodeID(rsp.GetNodeId())
	return
}

func convertTxID(txID yt.TxID) *misc.TGuid {
	return convertGUID(guid.GUID(txID))
}

func convertGUID(g guid.GUID) *misc.TGuid {
	first, second := g.Halves()
	return &misc.TGuid{
		First:  &first,
		Second: &second,
	}
}

func makeNodeID(g *misc.TGuid) yt.NodeID {
	return yt.NodeID(guid.FromHalves(g.GetFirst(), g.GetSecond()))
}

func convertAttributes(attrs map[string]interface{}) (*ytree.TAttributeDictionary, error) {
	if attrs == nil {
		return nil, nil
	}

	ret := &ytree.TAttributeDictionary{
		Attributes: make([]*ytree.TAttribute, 0, len(attrs)),
	}

	for key, value := range attrs {
		valueBytes, err := yson.Marshal(value)
		if err != nil {
			return nil, err
		}

		ret.Attributes = append(ret.Attributes, &ytree.TAttribute{
			Key:   ptr.String(key),
			Value: valueBytes,
		})
	}

	return ret, nil
}

func convertAttributeKeys(keys []string) *rpc_proxy.TAttributeKeys {
	if keys == nil {
		return &rpc_proxy.TAttributeKeys{All: ptr.Bool(true)}
	}

	return &rpc_proxy.TAttributeKeys{Columns: keys}
}

func convertTransactionOptions(opts *yt.TransactionOptions) *rpc_proxy.TTransactionalOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TTransactionalOptions{
		TransactionId: convertTxID(opts.TransactionID),
		Ping:          &opts.Ping,
		PingAncestors: &opts.PingAncestors,
	}
}

func convertPrerequisiteOptions(opts *yt.PrerequisiteOptions) *rpc_proxy.TPrerequisiteOptions {
	if opts == nil {
		return nil
	}

	txIDs := make([]*rpc_proxy.TPrerequisiteOptions_TTransactionPrerequisite, 0, len(opts.TransactionIDs))
	for _, id := range opts.TransactionIDs {
		txIDs = append(txIDs, &rpc_proxy.TPrerequisiteOptions_TTransactionPrerequisite{
			TransactionId: convertTxID(id),
		})
	}

	revisions := make([]*rpc_proxy.TPrerequisiteOptions_TRevisionPrerequisite, 0, len(opts.Revisions))
	for _, rev := range opts.Revisions {
		revisions = append(revisions, &rpc_proxy.TPrerequisiteOptions_TRevisionPrerequisite{
			Path:     ptr.String(rev.Path.String()),
			Revision: ptr.Uint64(uint64(rev.Revision)),
		})
	}

	return &rpc_proxy.TPrerequisiteOptions{
		Transactions: txIDs,
		Revisions:    revisions,
	}
}

func convertMasterReadOptions(opts *yt.MasterReadOptions) *rpc_proxy.TMasterReadOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TMasterReadOptions{
		ReadFrom: convertReadKind(opts.ReadFrom),
	}
}

func convertReadKind(k yt.ReadKind) *rpc_proxy.EMasterReadKind {
	var ret rpc_proxy.EMasterReadKind

	switch k {
	case yt.ReadFromLeader:
		ret = rpc_proxy.EMasterReadKind_MRK_LEADER
	case yt.ReadFromFollower:
		ret = rpc_proxy.EMasterReadKind_MRK_FOLLOWER
	case yt.ReadFromCache:
		ret = rpc_proxy.EMasterReadKind_MRK_CACHE
	// todo EMasterReadKind_MRK_MASTER_CACHE
	default:
		return nil
	}

	return &ret
}

func convertAccessTrackingOptions(opts *yt.AccessTrackingOptions) *rpc_proxy.TSuppressableAccessTrackingOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TSuppressableAccessTrackingOptions{
		SuppressAccessTracking:       &opts.SuppressAccessTracking,
		SuppressModificationTracking: &opts.SuppressModificationTracking,
	}
}

func convertMutatingOptions(opts *yt.MutatingOptions) *rpc_proxy.TMutatingOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TMutatingOptions{
		MutationId: convertGUID(guid.GUID(opts.MutationID)),
		Retry:      &opts.Retry,
	}
}
