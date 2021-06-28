package rpcclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
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
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal"
	"a.yandex-team.ru/yt/go/yterrors"
)

var _ yt.Client = (*client)(nil)

type client struct {
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

func NewClient(conf *yt.Config) (*client, error) {
	c := &client{
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

func (c *client) schema() string {
	schema := "http"
	if c.conf.UseTLS {
		schema = "https"
	}
	return schema
}

func (c *client) listRPCProxies() ([]string, error) {
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

func (c *client) do(
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

func (c *client) shouldBanProxy(err error) bool {
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

func (c *client) Stop() {
	_ = c.connPool.Close()
	c.stop.Stop()
}

func (c *client) CreateNode(
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

func (c *client) CreateObject(ctx context.Context, typ yt.NodeType, opts *yt.CreateObjectOptions) (id yt.NodeID, err error) {
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

func (c *client) NodeExists(
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

func (c *client) RemoveNode(
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

func (c *client) GetNode(
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

func (c *client) SetNode(
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

func (c *client) MultisetAttributes(
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

func (c *client) ListNode(
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

func (c *client) CopyNode(
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

func (c *client) MoveNode(
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

func (c *client) LinkNode(
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

var _ yt.FileClient = (*client)(nil)

func (c *client) WriteFile(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.WriteFileOptions,
) (w io.WriteCloser, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) ReadFile(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.ReadFileOptions,
) (r io.ReadCloser, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) PutFileToCache(
	ctx context.Context,
	path ypath.YPath,
	md5 string,
	opts *yt.PutFileToCacheOptions,
) (cachedPath ypath.YPath, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) GetFileFromCache(
	ctx context.Context,
	md5 string,
	opts *yt.GetFileFromCacheOptions,
) (path ypath.YPath, err error) {
	return ypath.Path(""), xerrors.New("implement me")
}

var _ yt.TableClient = (*client)(nil)

func (c *client) WriteTable(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.WriteTableOptions,
) (w yt.TableWriter, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) ReadTable(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.ReadTableOptions,
) (r yt.TableReader, err error) {
	return nil, xerrors.New("implement me")
}

var _ yt.TabletClient = (*client)(nil)

func (c *client) SelectRows(
	ctx context.Context,
	query string,
	opts *yt.SelectRowsOptions,
) (r yt.TableReader, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) LookupRows(
	ctx context.Context,
	path ypath.Path,
	keys []interface{},
	opts *yt.LookupRowsOptions,
) (r yt.TableReader, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) LockRows(
	ctx context.Context,
	path ypath.Path,
	locks []string,
	lockType yt.LockType,
	keys []interface{},
	opts *yt.LockRowsOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) InsertRows(
	ctx context.Context,
	path ypath.Path,
	rows []interface{},
	opts *yt.InsertRowsOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) DeleteRows(
	ctx context.Context,
	path ypath.Path,
	keys []interface{},
	opts *yt.DeleteRowsOptions,
) (err error) {
	return xerrors.New("implement me")
}

var _ yt.MountClient = (*client)(nil)

func (c *client) MountTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.MountTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) UnmountTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.UnmountTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) RemountTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.RemountTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) ReshardTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.ReshardTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) AlterTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.AlterTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) FreezeTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.FreezeTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) UnfreezeTable(
	ctx context.Context,
	path ypath.Path,
	opts *yt.UnfreezeTableOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) AlterTableReplica(
	ctx context.Context,
	id yt.NodeID,
	opts *yt.AlterTableReplicaOptions,
) (err error) {
	return xerrors.New("implement me")
}

func (c *client) StartTx(
	ctx context.Context,
	opts *yt.StartTxOptions,
) (id yt.TxID, err error) {
	if opts == nil {
		opts = &yt.StartTxOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	attrs, err := convertAttributes(opts.Attributes)
	if err != nil {
		err = xerrors.Errorf("unable to serialize attributes: %w", err)
		return
	}

	txType := rpc_proxy.ETransactionType_TT_MASTER
	req := &rpc_proxy.TReqStartTransaction{
		Type:                       &txType,
		Timeout:                    convertDuration(opts.Timeout),
		Id:                         nil, // todo
		ParentId:                   nil, // todo
		AutoAbort:                  nil, // todo
		Sticky:                     &opts.Sticky,
		Ping:                       &opts.Ping,
		PingAncestors:              &opts.PingAncestors,
		Atomicity:                  nil, // todo
		Durability:                 nil, // todo
		Attributes:                 attrs,
		Deadline:                   convertTime(opts.Deadline),
		PrerequisiteTransactionIds: convertPrerequisiteTxIDs(opts.PrerequisiteTransactionIDs),
	}

	var rsp rpc_proxy.TRspStartTransaction
	err = c.do(ctx, "StartTransaction", req, &rsp)
	if err != nil {
		return
	}

	id = yt.TxID(makeNodeID(rsp.GetId()))
	return
}

func (c *client) StartTabletTx(
	ctx context.Context,
	opts *yt.StartTabletTxOptions,
) (id yt.TxID, err error) {
	if opts == nil {
		opts = &yt.StartTabletTxOptions{
			Sticky: true,
		}
	}

	atomicity, err := convertAtomicity(opts.Atomicity)
	if err != nil {
		return
	}

	txType := rpc_proxy.ETransactionType_TT_TABLET
	req := &rpc_proxy.TReqStartTransaction{
		Type:                       &txType,
		Timeout:                    convertDuration(opts.Timeout),
		Id:                         nil, // todo
		ParentId:                   nil, // todo
		AutoAbort:                  nil, // todo
		Sticky:                     &opts.Sticky,
		Ping:                       nil, // todo
		PingAncestors:              nil, // todo
		Atomicity:                  atomicity,
		Durability:                 nil, // todo
		Attributes:                 nil, // todo
		Deadline:                   nil, // todo
		PrerequisiteTransactionIds: nil, // todo
	}

	var rsp rpc_proxy.TRspStartTransaction
	err = c.do(ctx, "StartTransaction", req, &rsp)
	if err != nil {
		return
	}

	id = yt.TxID(makeNodeID(rsp.GetId()))
	return
}

func (c *client) PingTx(
	ctx context.Context,
	id yt.TxID,
	opts *yt.PingTxOptions,
) (err error) {
	if opts == nil {
		opts = &yt.PingTxOptions{}
	}
	if opts.TransactionOptions == nil {
		opts.TransactionOptions = &yt.TransactionOptions{}
	}

	req := &rpc_proxy.TReqPingTransaction{
		TransactionId: convertTxID(id),
		PingAncestors: &opts.PingAncestors,
	}

	var rsp rpc_proxy.TRspPingTransaction
	err = c.do(ctx, "PingTransaction", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) AbortTx(
	ctx context.Context,
	id yt.TxID,
	opts *yt.AbortTxOptions,
) (err error) {
	req := &rpc_proxy.TReqAbortTransaction{
		TransactionId: convertTxID(id),
	}

	var rsp rpc_proxy.TRspAbortTransaction
	err = c.do(ctx, "AbortTransaction", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) CommitTx(
	ctx context.Context,
	id yt.TxID,
	opts *yt.CommitTxOptions,
) (err error) {
	if opts == nil {
		opts = &yt.CommitTxOptions{}
	}

	req := &rpc_proxy.TReqCommitTransaction{
		TransactionId:       convertTxID(id),
		PrerequisiteOptions: convertPrerequisiteOptions(opts.PrerequisiteOptions),
	}

	var rsp rpc_proxy.TRspCommitTransaction
	err = c.do(ctx, "CommitTransaction", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) AddMember(
	ctx context.Context,
	group string,
	member string,
	opts *yt.AddMemberOptions,
) (err error) {
	if opts == nil {
		opts = &yt.AddMemberOptions{}
	}

	req := &rpc_proxy.TReqAddMember{
		Group:               &group,
		Member:              &member,
		MutatingOptions:     convertMutatingOptions(opts.MutatingOptions),
		PrerequisiteOptions: convertPrerequisiteOptions(opts.PrerequisiteOptions),
	}

	var rsp rpc_proxy.TRspAddMember
	err = c.do(ctx, "AddMember", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) RemoveMember(
	ctx context.Context,
	group string,
	member string,
	opts *yt.RemoveMemberOptions,
) (err error) {
	if opts == nil {
		opts = &yt.RemoveMemberOptions{}
	}

	req := &rpc_proxy.TReqRemoveMember{
		Group:               &group,
		Member:              &member,
		MutatingOptions:     convertMutatingOptions(opts.MutatingOptions),
		PrerequisiteOptions: convertPrerequisiteOptions(opts.PrerequisiteOptions),
	}

	var rsp rpc_proxy.TRspRemoveMember
	err = c.do(ctx, "RemoveMember", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) StartOperation(
	ctx context.Context,
	opType yt.OperationType,
	spec interface{},
	opts *yt.StartOperationOptions,
) (opID yt.OperationID, err error) {
	if opts == nil {
		opts = &yt.StartOperationOptions{}
	}

	operationType, err := convertOperationType(&opType)
	if err != nil {
		return
	}

	specBytes, err := yson.Marshal(spec)
	if err != nil {
		err = xerrors.Errorf("unable to serialize spec: %w", err)
		return
	}

	req := &rpc_proxy.TReqStartOperation{
		Type:                 operationType,
		Spec:                 specBytes,
		TransactionalOptions: convertTransactionOptions(opts.TransactionOptions),
		MutatingOptions:      convertMutatingOptions(opts.MutatingOptions),
	}

	var rsp rpc_proxy.TRspStartOperation
	err = c.do(ctx, "StartOperation", req, &rsp)
	if err != nil {
		return
	}

	opID = yt.OperationID(makeNodeID(rsp.GetOperationId()))
	return
}

func (c *client) AbortOperation(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.AbortOperationOptions,
) (err error) {
	if opts == nil {
		opts = &yt.AbortOperationOptions{}
	}

	req := &rpc_proxy.TReqAbortOperation{
		OperationIdOrAlias: &rpc_proxy.TReqAbortOperation_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
		AbortMessage: opts.AbortMessage,
	}

	var rsp rpc_proxy.TRspAbortOperation
	err = c.do(ctx, "AbortOperation", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) SuspendOperation(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.SuspendOperationOptions,
) (err error) {
	if opts == nil {
		opts = &yt.SuspendOperationOptions{}
	}

	req := &rpc_proxy.TReqSuspendOperation{
		OperationIdOrAlias: &rpc_proxy.TReqSuspendOperation_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
		AbortRunningJobs: &opts.AbortRunningJobs,
	}

	var rsp rpc_proxy.TRspSuspendOperation
	err = c.do(ctx, "SuspendOperation", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) ResumeOperation(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.ResumeOperationOptions,
) (err error) {
	req := &rpc_proxy.TReqResumeOperation{
		OperationIdOrAlias: &rpc_proxy.TReqResumeOperation_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
	}

	var rsp rpc_proxy.TRspResumeOperation
	err = c.do(ctx, "ResumeOperation", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) CompleteOperation(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.CompleteOperationOptions,
) (err error) {
	req := &rpc_proxy.TReqCompleteOperation{
		OperationIdOrAlias: &rpc_proxy.TReqCompleteOperation_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
	}

	var rsp rpc_proxy.TRspCompleteOperation
	err = c.do(ctx, "CompleteOperation", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) UpdateOperationParameters(
	ctx context.Context,
	opID yt.OperationID,
	params interface{},
	opts *yt.UpdateOperationParametersOptions,
) (err error) {
	paramsBytes, err := yson.Marshal(params)
	if err != nil {
		err = xerrors.Errorf("unable to serialize params: %w", err)
		return
	}

	req := &rpc_proxy.TReqUpdateOperationParameters{
		OperationIdOrAlias: &rpc_proxy.TReqUpdateOperationParameters_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
		Parameters: paramsBytes,
	}

	var rsp rpc_proxy.TRspUpdateOperationParameters
	err = c.do(ctx, "UpdateOperationParameters", req, &rsp)
	if err != nil {
		return
	}

	return
}

func (c *client) GetOperation(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.GetOperationOptions,
) (status *yt.OperationStatus, err error) {
	if opts == nil {
		opts = &yt.GetOperationOptions{}
	}

	req := &rpc_proxy.TReqGetOperation{
		OperationIdOrAlias: &rpc_proxy.TReqGetOperation_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
		Attributes:                opts.Attributes,
		IncludeRuntime:            opts.IncludeRuntime,
		MaximumCypressProgressAge: nil, // todo
		MasterReadOptions:         convertMasterReadOptions(opts.MasterReadOptions),
	}

	var rsp rpc_proxy.TRspGetOperation
	err = c.do(ctx, "GetOperation", req, &rsp)
	if err != nil {
		return
	}

	status = &yt.OperationStatus{}
	if err := yson.Unmarshal(rsp.Meta, status); err != nil {
		return nil, err
	}

	return
}

func (c *client) ListOperations(
	ctx context.Context,
	opts *yt.ListOperationsOptions,
) (operations *yt.ListOperationsResult, err error) {
	if opts == nil {
		opts = &yt.ListOperationsOptions{}
	}

	opState, err := convertOperationState(opts.State)
	if err != nil {
		return nil, err
	}

	opType, err := convertOperationType(opts.Type)
	if err != nil {
		return nil, err
	}

	var limit *uint64
	if opts.Limit != nil {
		limit = ptr.Uint64(uint64(*opts.Limit))
	}

	req := &rpc_proxy.TReqListOperations{
		FromTime:               convertTime(opts.FromTime),
		ToTime:                 convertTime(opts.ToTime),
		CursorTime:             convertTime(opts.Cursor),
		CursorDirection:        nil, // todo
		UserFilter:             opts.User,
		StateFilter:            opState,
		TypeFilter:             opType,
		SubstrFilter:           opts.Filter,
		Pool:                   nil,   // todo
		IncludeArchive:         nil,   // todo
		IncludeCounters:        nil,   // todo
		Limit:                  limit, // todo
		Attributes:             nil,   // todo
		AccessFilter:           nil,   // todo
		ArchiveFetchingTimeout: nil,   // todo
		MasterReadOptions:      convertMasterReadOptions(opts.MasterReadOptions),
	}

	var rsp rpc_proxy.TRspListOperations
	err = c.do(ctx, "ListOperations", req, &rsp)
	if err != nil {
		return
	}

	operations, err = makeListOperationsResult(rsp.Result)
	if err != nil {
		return nil, xerrors.Errorf("unable to deserializer response: %w", err)
	}

	return
}

func (c *client) ListJobs(
	ctx context.Context,
	opID yt.OperationID,
	opts *yt.ListJobsOptions,
) (r *yt.ListJobsResult, err error) {
	if opts == nil {
		opts = &yt.ListJobsOptions{}
	}

	jobType, err := convertJobType(opts.JobType)
	if err != nil {
		return nil, err
	}

	jobState, err := convertJobState(opts.JobState)
	if err != nil {
		return nil, err
	}

	sortOrder, err := convertJobSortOrder(opts.SortOrder)
	if err != nil {
		return nil, err
	}

	var limit *int64
	if opts.Limit != nil {
		limit = ptr.Int64(int64(*opts.Limit))
	}

	var offset *int64
	if opts.Offset != nil {
		offset = ptr.Int64(int64(*opts.Offset))
	}

	dataSource, err := convertDataSource(opts.DataSource)
	if err != nil {
		return nil, err
	}

	req := &rpc_proxy.TReqListJobs{
		OperationIdOrAlias: &rpc_proxy.TReqListJobs_OperationId{
			OperationId: convertGUID(guid.GUID(opID)),
		},
		Type:                        jobType,
		State:                       jobState,
		Address:                     opts.Address,
		WithStderr:                  opts.WithStderr,
		WithFailContext:             opts.WithFailContext,
		WithSpec:                    nil, // todo
		SortField:                   nil, // todo
		SortOrder:                   sortOrder,
		Limit:                       limit,
		Offset:                      offset,
		IncludeCypress:              nil, // todo
		IncludeControllerAgent:      nil, // todo
		IncludeArchive:              nil, // todo
		DataSource:                  dataSource,
		RunningJobsLookbehindPeriod: nil, // todo
		JobCompetitionId:            nil, // todo
		WithCompetitors:             nil, // todo
		TaskName:                    nil, // todo
		MasterReadOptions:           nil, // todo
	}

	var rsp rpc_proxy.TRspListJobs
	err = c.do(ctx, "ListJobs", req, &rsp)
	if err != nil {
		return
	}

	r, err = makeListJobsResult(rsp.Result)
	if err != nil {
		return nil, xerrors.Errorf("unable to deserializer response: %w", err)
	}

	return
}

func (c *client) GetJobStderr(
	ctx context.Context,
	opID yt.OperationID,
	jobID yt.JobID,
	opts *yt.GetJobStderrOptions,
) (r []byte, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) GenerateTimestamp(
	ctx context.Context,
	opts *yt.GenerateTimestampOptions,
) (ts yt.Timestamp, err error) {
	return 0, xerrors.New("implement me")
}

func (c *client) LocateSkynetShare(
	ctx context.Context,
	path ypath.YPath,
	opts *yt.LocateSkynetShareOptions,
) (l yt.ShareLocation, err error) {
	return yt.ShareLocation{}, xerrors.New("implement me")
}

func (c *client) GetInSyncReplicas(
	ctx context.Context,
	path ypath.Path,
	ts yt.Timestamp,
	keys []interface{},
	opts *yt.GetInSyncReplicasOptions,
) (ids []yt.NodeID, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) BeginTx(
	ctx context.Context,
	opts *yt.StartTxOptions,
) (tx yt.Tx, err error) {
	return nil, xerrors.New("implement me")
}

func (c *client) BeginTabletTx(
	ctx context.Context,
	opts *yt.StartTabletTxOptions,
) (tx yt.TabletTx, err error) {
	return nil, xerrors.New("implement me")
}
