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

static const auto& Logger = RpcProxyClientLogger;

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
        req->set_timeout(ToProto(*options.Timeout));
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

TFuture<NYson::TYsonString> TRpcProxyClientBase::GetNode(
    const NYPath::TYPath& path,
    const TGetNodeOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.GetNode();
    req->set_path(path);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetNodePtr>& rspOrError) -> NYson::TYsonString {
        const auto& rsp = rspOrError.ValueOrThrow();
        return NYson::TYsonString(rsp->data());
    }));
}

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
            req->add_columns(Stroka(nameTable->GetName(id)));
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
            req->add_columns(Stroka(nameTable->GetName(id)));
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
    const Stroka& query,
    const NApi::TSelectRowsOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.SelectRows();
    req->set_query(query);
    req->SetTimeout(options.Timeout);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspSelectRowsPtr>& rspOrError) -> NApi::TSelectRowsResult {
        const auto& rsp = rspOrError.ValueOrThrow();
        TSelectRowsResult result;
        result.Rowset = DeserializeRowset<TUnversionedRow>(
            rsp->rowset_descriptor(),
            MergeRefsToRef<TRpcProxyClientBaseBufferTag>(rsp->Attachments()));
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
