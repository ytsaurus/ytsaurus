#include "rpc_proxy_client.h"
#include "api_service_proxy.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/net/address.h>

#include <yt/core/misc/small_set.h>

#include <yt/core/ytree/convert.h>

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
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyClient::TRpcProxyClient(
    TRpcProxyConnectionPtr connection,
    const TClientOptions& options)
    : Connection_(std::move(connection))
    , Channel_(CreateRpcProxyChannel(Connection_, options))
{ }

TRpcProxyClient::~TRpcProxyClient() = default;

TRpcProxyConnectionPtr TRpcProxyClient::GetRpcProxyConnection()
{
    return Connection_;
}

IChannelPtr TRpcProxyClient::GetChannel()
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
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ReshardTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    TWireProtocolWriter writer;
    // XXX(sandello): This is ugly and inefficient.
    std::vector<TUnversionedRow> keys;
    for (const auto& pivotKey : pivotKeys) {
        keys.push_back(pivotKey);
    }
    writer.WriteRowset(MakeRange(keys));
    req->Attachments() = writer.Finish();

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::ReshardTable(
    const NYPath::TYPath& path,
    int tabletCount,
    const NApi::TReshardTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.ReshardTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_tablet_count(tabletCount);

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_tablet_range_options(), options);

    return req->Invoke().As<void>();;
}

TFuture<void> TRpcProxyClient::TrimTable(
    const NYPath::TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const NApi::TTrimTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.TrimTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    req->set_tablet_index(tabletIndex);
    req->set_trimmed_row_count(trimmedRowCount);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::AlterTable(
    const NYPath::TYPath& path,
    const NApi::TAlterTableOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.AlterTable();
    SetTimeoutOptions(*req, options);

    req->set_path(path);

    if (options.Schema) {
        req->set_schema(ConvertToYsonString(*options.Schema).GetData());
    }
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }

    ToProto(req->mutable_mutating_options(), options);
    ToProto(req->mutable_transactional_options(), options);

    return req->Invoke().As<void>();
}

TFuture<void> TRpcProxyClient::AlterTableReplica(
    const NTabletClient::TTableReplicaId& replicaId,
    const NApi::TAlterTableReplicaOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.AlterTableReplica();
    SetTimeoutOptions(*req, options);

    ToProto(req->mutable_replica_id(), replicaId);

    if (options.Enabled) {
        req->set_enabled(*options.Enabled);
    }
    if (options.Mode) {
        switch (*options.Mode) {
            case ETableReplicaMode::Sync:
                req->set_mode(NProto::TReqAlterTableReplica_ETableReplicaMode_SYNC);
                break;
            case ETableReplicaMode::Async:
                req->set_mode(NProto::TReqAlterTableReplica_ETableReplicaMode_ASYNC);
                break;
        }
    }

    return req->Invoke().As<void>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
