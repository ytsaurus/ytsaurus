#include "rpc_proxy_client.h"
#include "rpc_proxy_connection.h"
#include "credentials_injecting_channel.h"
#include "api_service_proxy.h"
#include "private.h"

#include <yt/core/misc/address.h>
#include <yt/core/misc/small_set.h>
#include <yt/core/misc/serialize.h>

#include <yt/ytlib/api/connection.h>
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

TFuture<NApi::IRowsetPtr> TRpcProxyClient::LookupRows(
    const NYPath::TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TLookupRowsOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.LookupRows();
    req->set_path(path);

    for (size_t id = 0; id < nameTable->GetSize(); ++id) {
        auto* desc = req->add_rowset_column_descriptor();
        desc->set_id(id);
        desc->set_name(Stroka(nameTable->GetName(id)));
    }

    req->set_wire_format_version(1);
    TWireProtocolWriter writer;
    writer.WriteUnversionedRowset(keys);
    req->Attachments() = writer.Finish();

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspLookupRowsPtr>& rspOrError) -> NApi::IRowsetPtr {
        const auto& rsp = rspOrError.ValueOrThrow();

        std::vector<TColumnSchema> columns;
        columns.resize(rsp->rowset_column_descriptor_size());
        for (const auto& desc : rsp->rowset_column_descriptor()) {
            auto& column = columns[desc.id()];
            if (desc.has_name()) {
                column.Name = desc.name();
            }
            if (desc.has_type()) {
                column.Type = EValueType(desc.type());
            }
        }

        TTableSchema schema(std::move(columns));
        TWireProtocolReader reader(
            MergeRefsToRef<TRpcProxyClientBufferTag>(rsp->Attachments()),
            New<TRowBuffer>(TRpcProxyClientBufferTag()));

        auto rows = reader.ReadUnversionedRowset(true);
        return NApi::CreateRowset(std::move(schema), std::move(rows));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
