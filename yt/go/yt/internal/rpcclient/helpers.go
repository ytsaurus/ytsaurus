package rpcclient

import (
	"encoding/json"
	"net/http"
	"time"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	"a.yandex-team.ru/yt/go/proto/core/ytree"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

// unexpectedStatusCode is last effort attempt to get useful error message from a failed request.
func unexpectedStatusCode(rsp *http.Response) error {
	d := json.NewDecoder(rsp.Body)
	d.UseNumber()

	var ytErr yterrors.Error
	if err := d.Decode(&ytErr); err == nil {
		return &ytErr
	}

	return xerrors.Errorf("unexpected status code %d", rsp.StatusCode)
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

func convertPrerequisiteTxIDs(ids []yt.TxID) []*misc.TGuid {
	if ids == nil {
		return nil
	}

	txIDs := make([]*misc.TGuid, 0, len(ids))
	for _, id := range ids {
		txIDs = append(txIDs, convertTxID(id))
	}

	return txIDs
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

func convertDuration(d *yson.Duration) *int64 {
	if d == nil {
		return nil
	}

	return ptr.Int64(int64(time.Duration(*d) / time.Microsecond))
}

func convertTime(t *yson.Time) *uint64 {
	if t == nil {
		return nil
	}

	return ptr.Uint64(uint64(time.Time(*t).UTC().UnixNano() / time.Hour.Microseconds()))
}

func convertAtomicity(a *yt.Atomicity) (*rpc_proxy.EAtomicity, error) {
	if a == nil {
		return nil, nil
	}

	var ret rpc_proxy.EAtomicity

	switch *a {
	case yt.AtomicityNone:
		ret = rpc_proxy.EAtomicity_A_NONE
	case yt.AtomicityFull:
		ret = rpc_proxy.EAtomicity_A_FULL
	default:
		return nil, xerrors.Errorf("unexpected atomicity %q", *a)
	}

	return &ret, nil
}
