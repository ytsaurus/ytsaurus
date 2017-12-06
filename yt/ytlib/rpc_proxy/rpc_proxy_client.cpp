#include "rpc_proxy_client.h"
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

using NYT::ToProto;
using NYT::FromProto;

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
    const TYPath& path,
    const TMountTableOptions& options)
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
    const TYPath& path,
    const TUnmountTableOptions& options)
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
    const TYPath& path,
    const TRemountTableOptions& options)
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
    const TYPath& path,
    const TFreezeTableOptions& options)
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
    const TYPath& path,
    const TUnfreezeTableOptions& options)
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
    const TYPath& path,
    const std::vector<NTableClient::TOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
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
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
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
    const TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& options)
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
    const TYPath& path,
    const TAlterTableOptions& options)
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
    const TTableReplicaId& replicaId,
    const TAlterTableReplicaOptions& options)
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

TFuture<std::vector<TTableReplicaId>> TRpcProxyClient::GetInSyncReplicas(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TKey>& keys,
    const TGetInSyncReplicasOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.GetInSyncReplicas();
    SetTimeoutOptions(*req, options);

    if (options.Timestamp) {
        req->set_timestamp(options.Timestamp);
    }

    req->set_path(path);
    req->Attachments() = SerializeRowset(nameTable, keys, req->mutable_rowset_descriptor());

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetInSyncReplicasPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        return FromProto<std::vector<TTableReplicaId>>(rsp->replica_ids());
    }));
}

TFuture<std::vector<NApi::TTabletInfo>> TRpcProxyClient::GetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletsInfoOptions& options)
{
    TApiServiceProxy proxy(GetChannel());

    auto req = proxy.GetTabletInfos();
    SetTimeoutOptions(*req, options);

    req->set_path(path);
    ToProto(req->mutable_tablet_indexes(), tabletIndexes);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetTabletInfosPtr>& rspOrError) {
        const auto& rsp = rspOrError.ValueOrThrow();
        std::vector<NApi::TTabletInfo> tabletInfos;
        tabletInfos.reserve(rsp->tablets_size());
        for (const auto& protoTabletInfo : rsp->tablets()) {
            tabletInfos.emplace_back();
            auto& result = tabletInfos.back();
            result.TotalRowCount = protoTabletInfo.total_row_count();
            result.TrimmedRowCount = protoTabletInfo.trimmed_row_count();
        }
        return tabletInfos;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
