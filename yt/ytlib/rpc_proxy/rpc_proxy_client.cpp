#include "rpc_proxy_client.h"
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

TRpcProxyClient::TRpcProxyClient(
    TRpcProxyConnectionPtr connection,
    NRpc::IChannelPtr channel)
    : Connection_(std::move(connection))
    , Channel_(std::move(channel))
{ }

TRpcProxyClient::~TRpcProxyClient()
{ }

TRpcProxyConnectionPtr TRpcProxyClient::GetRpcProxyConnection()
{
    return Connection_;
}

NRpc::IChannelPtr TRpcProxyClient::GetChannel()
{
    return Channel_;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TRpcProxyClient::MountTable(
    const NYPath::TYPath& path,
    const NApi::TMountTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.MountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    NYT::ToProto(req->mutable_cell_id(), options.CellId);
    req->set_freeze(options.Freeze);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::UnmountTable(
    const NYPath::TYPath& path,
    const NApi::TUnmountTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.UnmountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    req->set_force(options.Force);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::RemountTable(
    const NYPath::TYPath& path,
    const NApi::TRemountTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.RemountTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::FreezeTable(
    const NYPath::TYPath& path,
    const NApi::TFreezeTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.FreezeTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::UnfreezeTable(
    const NYPath::TYPath& path,
    const NApi::TUnfreezeTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.UnfreezeTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::ReshardTable(
    const NYPath::TYPath& path,
    const std::vector<NTableClient::TOwningKey>& pivotKeys,
    const NApi::TReshardTableOptions& options)
{
    Y_UNIMPLEMENTED();
}

TFuture<void> TRpcProxyClient::ReshardTable(
    const NYPath::TYPath& path,
    int tabletCount,
    const NApi::TReshardTableOptions& options)
{
    Y_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
