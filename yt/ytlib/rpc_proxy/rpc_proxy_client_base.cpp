#include "rpc_proxy_client_base.h"
#include "rpc_proxy_transaction.h"
#include "credentials_injecting_channel.h"
#include "api_service_proxy.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/misc/address.h>
#include <yt/core/misc/small_set.h>

#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/row_base.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

struct TRpcProxyClientBaseBufferTag
{ };

TRpcProxyClientBase::TRpcProxyClientBase()
{ }

TRpcProxyClientBase::~TRpcProxyClientBase()
{ }

TFuture<NApi::ITransactionPtr> TRpcProxyClientBase::StartTransaction(
    NTransactionClient::ETransactionType type,
    const NApi::TTransactionStartOptions& options)
{
    // Keep connection & channel to reuse them in the transaction.
    auto connection = GetRpcProxyConnection();
    auto channel = GetChannel();

    TApiServiceProxy proxy(channel);

    auto req = proxy.StartTransaction();
    req->set_type(NProto::ETransactionType(type));
    if (options.Timeout) {
        req->set_timeout(NYT::ToProto(*options.Timeout));
    }
    if (options.Id) {
        ToProto(req->mutable_id(), options.Id);
    }
    if (options.ParentId) {
        ToProto(req->mutable_parent_id(), options.ParentId);
    }
    req->set_auto_abort(options.AutoAbort);
    // XXX(sandello): Better?
    bool sticky = type == NTransactionClient::ETransactionType::Tablet
        ? true
        : options.Sticky;
    req->set_sticky(sticky);
    req->set_ping(options.Ping);
    req->set_ping_ancestors(options.PingAncestors);

    return req->Invoke().Apply(BIND(
        [connection = std::move(connection), channel = std::move(channel), sticky = sticky]
        (const TErrorOr<TApiServiceProxy::TRspStartTransactionPtr>& rspOrError) mutable -> NApi::ITransactionPtr {
            const auto& rsp = rspOrError.ValueOrThrow();
            auto transaction = New<TRpcProxyTransaction>(
                std::move(connection),
                std::move(channel),
                FromProto<TGuid>(rsp->id()),
                static_cast<TTimestamp>(rsp->start_timestamp()),
                sticky);
            // TODO(sandello): Register me in #connection to ping transaction periodically.
            return transaction;
        }));
}

////////////////////////////////////////////////////////////////////////////////
// CYPRESS
////////////////////////////////////////////////////////////////////////////////

TFuture<bool> TRpcProxyClientBase::NodeExists(
    const NYPath::TYPath& path,
    const NApi::TNodeExistsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ExistsNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspExistsNodePtr& rsp) -> bool {
        return rsp->exists();
    }));
}

TFuture<NYson::TYsonString> TRpcProxyClientBase::GetNode(
    const NYPath::TYPath& path,
    const TGetNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.GetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    auto* protoAttributes = req->mutable_attributes();
    if (!options.Attributes) {
        protoAttributes->set_all(true);
    } else {
        protoAttributes->set_all(false);
        for (const auto& attribute : *options.Attributes) {
            protoAttributes->add_columns(attribute);
        }
    }
    if (options.MaxSize) {
        req->set_max_size(*options.MaxSize);
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetNodePtr& rsp) {
        return NYson::TYsonString(rsp->value());
    }));
}

TFuture<NYson::TYsonString> TRpcProxyClientBase::ListNode(
    const NYPath::TYPath& path,
    const NApi::TListNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ListNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    auto* protoAttributes = req->mutable_attributes();
    if (!options.Attributes) {
        protoAttributes->set_all(true);
    } else {
        protoAttributes->set_all(false);
        for (const auto& attribute : *options.Attributes) {
            protoAttributes->add_columns(attribute);
        }
    }
    if (options.MaxSize) {
        req->set_max_size(*options.MaxSize);
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspListNodePtr& rsp) {
        return NYson::TYsonString(rsp->value());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::CreateNode(
    const NYPath::TYPath& path,
    NObjectClient::EObjectType type,
    const NApi::TCreateNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.CreateNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_type(static_cast<int>(type));

    if (options.Attributes) {
        auto* protoItem = req->mutable_attributes();
        for (const auto& key : options.Attributes->List()) {
            auto* protoAttribute = protoItem->add_attributes();
            protoAttribute->set_key(key);
            protoAttribute->set_value(options.Attributes->GetYson(key).GetData());
        }
    }
    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_ignore_existing(options.IgnoreExisting);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCreateNodePtr& rsp) {
        return FromProto<TGuid>(rsp->node_id());
    }));
}

TFuture<void> TRpcProxyClientBase::RemoveNode(
    const NYPath::TYPath& path,
    const NApi::TRemoveNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.RemoveNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspRemoveNodePtr& rsp) {
    }));
}

TFuture<void> TRpcProxyClientBase::SetNode(
    const NYPath::TYPath& path,
    const NYson::TYsonString& value,
    const TSetNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.SetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_value(value.GetData());

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspSetNodePtr& rsp) {
    }));
}

TFuture<TLockNodeResult> TRpcProxyClientBase::LockNode(
    const NYPath::TYPath& path,
    NCypressClient::ELockMode mode,
    const NApi::TLockNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.LockNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_mode(static_cast<int>(mode));

    req->set_waitable(options.Waitable);
    if (options.ChildKey) {
        req->set_child_key(*options.ChildKey);
    }
    if (options.AttributeKey) {
        req->set_attribute_key(*options.AttributeKey);
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspLockNodePtr& rsp) {
        TLockNodeResult result;
        FromProto(&result.NodeId, rsp->node_id());
        FromProto(&result.LockId, rsp->lock_id());
        return result;
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::CopyNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const NApi::TCopyNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.CopyNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCopyNodePtr& rsp) {
        return FromProto<TGuid>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::MoveNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const NApi::TMoveNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.MoveNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_preserve_account(options.PreserveAccount);
    req->set_preserve_expiration_time(options.PreserveExpirationTime);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspMoveNodePtr& rsp) {
        return FromProto<TGuid>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::LinkNode(
    const NYPath::TYPath& srcPath,
    const NYPath::TYPath& dstPath,
    const NApi::TLinkNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.LinkNode();
    SetTimeoutOptions(*req, options);

    req->set_src_path(srcPath);
    req->set_dst_path(dstPath);

    req->set_recursive(options.Recursive);
    req->set_force(options.Force);
    req->set_ignore_existing(options.IgnoreExisting);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspLinkNodePtr& rsp) {
        return FromProto<TGuid>(rsp->node_id());
    }));
}

TFuture<void> TRpcProxyClientBase::ConcatenateNodes(
    const std::vector<NYPath::TYPath>& srcPaths,
    const NYPath::TYPath& dstPath,
    const NApi::TConcatenateNodesOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ConcatenateNodes();
    SetTimeoutOptions(*req, options);

    for (const auto& srcPath : srcPaths) {
        req->add_src_path(srcPath);
    }
    req->set_dst_path(dstPath);

    req->set_append(options.Append);

    ToProto(req->mutable_transactional_options(), options);
    // ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspConcatenateNodesPtr& rsp) {
    }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NApi::IUnversionedRowsetPtr> TRpcProxyClientBase::LookupRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.LookupRows();
    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());
    req->SetTimeout(options.Timeout);

    if (!options.ColumnFilter.All) {
        for (auto id : options.ColumnFilter.Indexes) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspLookupRowsPtr>& rspOrError) -> NApi::IUnversionedRowsetPtr {
        const auto& rsp = rspOrError.ValueOrThrow();
        return DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBaseBufferTag>(rsp->Attachments()));
    }));
}

TFuture<NApi::IVersionedRowsetPtr> TRpcProxyClientBase::VersionedLookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const NApi::TVersionedLookupRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.VersionedLookupRows();
    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());
    req->SetTimeout(options.Timeout);

    if (!options.ColumnFilter.All) {
        for (auto id : options.ColumnFilter.Indexes) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspVersionedLookupRowsPtr>& rspOrError) -> NApi::IVersionedRowsetPtr {
        const auto& rsp = rspOrError.ValueOrThrow();
        return DeserializeRowset<TVersionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBaseBufferTag>(rsp->Attachments()));
    }));
}

TFuture<NApi::TSelectRowsResult> TRpcProxyClientBase::SelectRows(
    const TString& query,
    const NApi::TSelectRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.SelectRows();
    req->set_query(query);
    req->SetTimeout(options.Timeout);

    req->set_timestamp(options.Timestamp);
    if (options.InputRowLimit) {
        req->set_input_row_limit(*options.InputRowLimit);
    }
    if (options.OutputRowLimit) {
        req->set_output_row_limit(*options.OutputRowLimit);
    }
    req->set_range_expansion_limit(options.RangeExpansionLimit);
    req->set_fail_on_incomplete_result(options.FailOnIncompleteResult);
    req->set_verbose_logging(options.VerboseLogging);
    req->set_enable_code_cache(options.EnableCodeCache);
    req->set_max_subqueries(options.MaxSubqueries);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspSelectRowsPtr>& rspOrError) -> NApi::TSelectRowsResult {
        const auto& rsp = rspOrError.ValueOrThrow();
        TSelectRowsResult result;
        result.Rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBaseBufferTag>(rsp->Attachments()));
        return result;
    }));
}

TFuture<std::vector<NTabletClient::TTableReplicaId>> TRpcProxyClientBase::GetInSyncReplicas(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const NApi::TGetInSyncReplicasOptions& options)
{
    Y_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
