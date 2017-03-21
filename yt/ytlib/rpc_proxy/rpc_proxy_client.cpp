#include "rpc_proxy_client.h"
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

struct TRpcProxyClientBufferTag
{ };

TRpcProxyClient::TRpcProxyClient(
    TRpcProxyConnectionPtr connection,
    const TClientOptions& options)
    : Connection_(std::move(connection))
{
    Channel_ = Connection_->Channel_;

    // TODO(sandello): Extract this to a new TAddressResolver method.
    auto localHostname = TAddressResolver::Get()->GetLocalHostName();
    auto localAddress = TAddressResolver::Get()->Resolve(localHostname).Get().ValueOrThrow();

    auto localAddressString = ToString(localAddress);
    YCHECK(localAddressString.StartsWith("tcp://"));
    localAddressString = localAddressString.substr(6);
    {
        auto index = localAddressString.rfind(':');
        if (index != Stroka::npos) {
            localAddressString = localAddressString.substr(0, index);
        }
    }
    if (localAddressString.StartsWith("[") && localAddressString.EndsWith("]")) {
        localAddressString = localAddressString.substr(1, localAddressString.length() - 2);
    }

    LOG_DEBUG("Originating address is %v", localAddressString);

    if (options.Token) {
        Channel_ = CreateTokenInjectingChannel(
            Channel_,
            options.User,
            *options.Token,
            localAddressString);
    } else if (options.SessionId || options.SslSessionId) {
        Channel_ = CreateCookieInjectingChannel(
            Channel_,
            options.User,
            "yt.yandex-team.ru", // TODO(sandello): where to get this?
            options.SessionId.Get(Stroka()),
            options.SslSessionId.Get(Stroka()),
            localAddressString);
    }
}

TRpcProxyClient::~TRpcProxyClient()
{ }

TFuture<NYson::TYsonString> TRpcProxyClient::GetNode(
    const NYPath::TYPath& path,
    const TGetNodeOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.GetNode();
    req->set_path(path);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetNodePtr>& rspOrError) -> NYson::TYsonString {
        const auto& rsp = rspOrError.ValueOrThrow();
        return NYson::TYsonString(rsp->data());
    }));
}

TFuture<NApi::IUnversionedRowsetPtr> TRpcProxyClient::LookupRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    TApiServiceProxy proxy(Channel_);

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
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
    }));
}

TFuture<NApi::IVersionedRowsetPtr> TRpcProxyClient::VersionedLookupRows(
    const NYPath::TYPath& path,
    NTableClient::TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const NApi::TVersionedLookupRowsOptions& options)
{
    TApiServiceProxy proxy(Channel_);

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
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
