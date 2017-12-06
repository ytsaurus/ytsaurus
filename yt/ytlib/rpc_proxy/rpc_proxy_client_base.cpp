#include "rpc_proxy_client_base.h"
#include "rpc_proxy_transaction.h"
#include "credentials_injecting_channel.h"
#include "api_service_proxy.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/net/address.h>

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
using namespace NYPath;
using namespace NYson;
using namespace NTableClient;
using namespace NTabletClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr TRpcProxyClientBase::GetConnection()
{
    return GetRpcProxyConnection();
}

TFuture<ITransactionPtr> TRpcProxyClientBase::StartTransaction(
    NTransactionClient::ETransactionType type,
    const TTransactionStartOptions& options)
{
    // Keep connection & channel to reuse them in the transaction.
    auto connection = GetRpcProxyConnection();
    auto channel = GetChannel();

    TApiServiceProxy proxy(channel);

    auto req = proxy.StartTransaction();
    req->set_type(NProto::ETransactionType(type));
    if (options.Timeout) {
        req->set_timeout(NYT::ToProto<i64>(*options.Timeout));
    }
    if (options.Id) {
        ToProto(req->mutable_id(), options.Id);
    }
    if (options.ParentId) {
        ToProto(req->mutable_parent_id(), options.ParentId);
    }
    // XXX(sandello): Better? Remove these fields from the protocol at all?
    req->set_auto_abort(false);
    bool sticky = type == NTransactionClient::ETransactionType::Tablet
        ? true
        : options.Sticky;
    req->set_sticky(sticky);
    req->set_ping(options.Ping);
    req->set_ping_ancestors(options.PingAncestors);
    req->set_atomicity(static_cast<NProto::EAtomicity>(options.Atomicity));
    req->set_durability(static_cast<NProto::EDurability>(options.Durability));

    return req->Invoke().Apply(BIND(
        [connection = std::move(connection), channel = std::move(channel), sticky = sticky]
        (const TErrorOr<TApiServiceProxy::TRspStartTransactionPtr>& rspOrError) -> ITransactionPtr {
            const auto& rsp = rspOrError.ValueOrThrow();
            return New<TRpcProxyTransaction>(
                std::move(connection),
                std::move(channel),
                FromProto<NTransactionClient::TTransactionId>(rsp->id()),
                static_cast<TTimestamp>(rsp->start_timestamp()),
                sticky);
        }));
}

////////////////////////////////////////////////////////////////////////////////
// CYPRESS
////////////////////////////////////////////////////////////////////////////////

TFuture<bool> TRpcProxyClientBase::NodeExists(
    const TYPath& path,
    const TNodeExistsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ExistsNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspExistsNodePtr& rsp) {
        return rsp->exists();
    }));
}

TFuture<TYsonString> TRpcProxyClientBase::GetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.GetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    auto* protoAttributes = req->mutable_attributes();
    if (options.Attributes) {
        protoAttributes->set_all(false);
        for (const auto& attribute : *options.Attributes) {
            protoAttributes->add_columns(attribute);
        }
    } else {
        protoAttributes->set_all(true);
    }
    if (options.MaxSize) {
        req->set_max_size(*options.MaxSize);
    }

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_master_read_options(), options);
    ToProto(req->mutable_suppressable_access_tracking_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspGetNodePtr& rsp) {
        return TYsonString(rsp->value());
    }));
}

TFuture<TYsonString> TRpcProxyClientBase::ListNode(
    const TYPath& path,
    const TListNodeOptions& options)
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
        return TYsonString(rsp->value());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::CreateNode(
    const TYPath& path,
    NObjectClient::EObjectType type,
    const TCreateNodeOptions& options)
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
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<void> TRpcProxyClientBase::RemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
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

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClientBase::SetNode(
    const TYPath& path,
    const TYsonString& value,
    const TSetNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.SetNode();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_value(value.GetData());
    req->set_recursive(options.Recursive);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

TFuture<TLockNodeResult> TRpcProxyClientBase::LockNode(
    const TYPath& path,
    NCypressClient::ELockMode mode,
    const TLockNodeOptions& options)
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
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TCopyNodeOptions& options)
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
    req->set_preserve_creation_time(options.PreserveCreationTime);

    ToProto(req->mutable_transactional_options(), options);
    ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().Apply(BIND([] (const TApiServiceProxy::TRspCopyNodePtr& rsp) {
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::MoveNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TMoveNodeOptions& options)
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
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<NCypressClient::TNodeId> TRpcProxyClientBase::LinkNode(
    const TYPath& srcPath,
    const TYPath& dstPath,
    const TLinkNodeOptions& options)
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
        return FromProto<NCypressClient::TNodeId>(rsp->node_id());
    }));
}

TFuture<void> TRpcProxyClientBase::ConcatenateNodes(
    const std::vector<TYPath>& srcPaths,
    const TYPath& dstPath,
    const TConcatenateNodesOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ConcatenateNodes();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_src_paths(), srcPaths);
    req->set_dst_path(dstPath);
    req->set_append(options.Append);
    ToProto(req->mutable_transactional_options(), options);
    // ToProto(req->mutable_prerequisite_options(), options);
    ToProto(req->mutable_mutating_options(), options);

    return req->Invoke().As<void>();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IUnversionedRowsetPtr> TRpcProxyClientBase::LookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.LookupRows();
    req->SetTimeout(options.Timeout);

    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    if (!options.ColumnFilter.All) {
        for (auto id : options.ColumnFilter.Indexes) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspLookupRowsPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
    }));
}

TFuture<IVersionedRowsetPtr> TRpcProxyClientBase::VersionedLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.VersionedLookupRows();
    req->SetTimeout(options.Timeout);

    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    if (!options.ColumnFilter.All) {
        for (auto id : options.ColumnFilter.Indexes) {
            req->add_columns(TString(nameTable->GetName(id)));
        }
    }
    req->set_timestamp(options.Timestamp);
    req->set_keep_missing_rows(options.KeepMissingRows);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspVersionedLookupRowsPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return DeserializeRowset<TVersionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
    }));
}

TFuture<TSelectRowsResult> TRpcProxyClientBase::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.SelectRows();
    req->SetTimeout(options.Timeout);

    req->set_query(query);

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

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspSelectRowsPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        TSelectRowsResult result;
        result.Rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
