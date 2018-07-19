#include "native_table_mount_cache.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/client/object_client/helpers.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/client/tablet_client/table_mount_cache.h>
#include <yt/client/tablet_client/table_mount_cache_detail.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/string.h>

#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/ytree/proto/ypath.pb.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NTabletClient {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NRpc;
using namespace NElection;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache
    : public TTableMountCacheBase
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel,
        TCellDirectoryPtr cellDirectory,
        const NLogging::TLogger& logger)
        : TTableMountCacheBase(std::move(config), logger)
        , CellDirectory_(std::move(cellDirectory))
        , ObjectProxy_(std::move(masterChannel))
    { }

private:

    virtual TFuture<TTableMountInfoPtr> DoGet(const TYPath& path) override
    {
        LOG_DEBUG("Requesting table mount info (Path: %v)",
            path);

        auto batchReq = ObjectProxy_.ExecuteBatch();

        auto* balancingHeaderExt = batchReq->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
        balancingHeaderExt->set_enable_stickness(true);
        balancingHeaderExt->set_sticky_group_size(1);

        auto req = TTableYPathProxy::GetMountInfo(path);

        auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
        cachingHeaderExt->set_success_expiration_time(ToProto<i64>(Config_->ExpireAfterSuccessfulUpdateTime));
        cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(Config_->ExpireAfterFailedUpdateTime));

        batchReq->AddRequest(req);
        return batchReq->Invoke().Apply(
            BIND([= , this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto error = GetCumulativeError(batchRspOrError);
                if (!error.IsOK()) {
                    auto wrappedError = TError("Error getting mount info for %v",
                        path)
                        << error;
                    LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                const auto& batchRsp = batchRspOrError.Value();
                const auto& rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspGetMountInfo>(0);
                const auto& rsp = rspOrError.Value();

                auto tableInfo = New<TTableMountInfo>();
                tableInfo->Path = path;
                tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());

                auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
                primarySchema = FromProto<TTableSchema>(rsp->schema());

                tableInfo->Schemas[ETableSchemaKind::Write] = primarySchema.ToWrite();
                tableInfo->Schemas[ETableSchemaKind::VersionedWrite] = primarySchema.ToVersionedWrite();
                tableInfo->Schemas[ETableSchemaKind::Delete] = primarySchema.ToDelete();
                tableInfo->Schemas[ETableSchemaKind::Query] = primarySchema.ToQuery();
                tableInfo->Schemas[ETableSchemaKind::Lookup] = primarySchema.ToLookup();

                tableInfo->UpstreamReplicaId = FromProto<TTableReplicaId>(rsp->upstream_replica_id());
                tableInfo->Dynamic = rsp->dynamic();
                tableInfo->NeedKeyEvaluation = primarySchema.HasComputedColumns();

                for (const auto& protoTabletInfo : rsp->tablets()) {
                    auto tabletInfo = New<TTabletInfo>();
                    tabletInfo->CellId = FromProto<TCellId>(protoTabletInfo.cell_id());
                    tabletInfo->TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
                    tabletInfo->MountRevision = protoTabletInfo.mount_revision();
                    tabletInfo->State = ETabletState(protoTabletInfo.state());
                    tabletInfo->UpdateTime = Now();

                    // COMPAT(savrus)
                    tabletInfo->InMemoryMode = protoTabletInfo.has_in_memory_mode()
                        ? MakeNullable(EInMemoryMode(protoTabletInfo.in_memory_mode()))
                        : Null;

                    if (tableInfo->IsSorted()) {
                        // Take the actual pivot from master response.
                        tabletInfo->PivotKey = FromProto<TOwningKey>(protoTabletInfo.pivot_key());
                    } else {
                        // Synthesize a fake pivot key.
                        TUnversionedOwningRowBuilder builder(1);
                        int tabletIndex = static_cast<int>(tableInfo->Tablets.size());
                        builder.AddValue(MakeUnversionedInt64Value(tabletIndex));
                        tabletInfo->PivotKey = builder.FinishRow();
                    }

                    if (protoTabletInfo.has_cell_id()) {
                        tabletInfo->CellId = FromProto<TCellId>(protoTabletInfo.cell_id());
                    }

                    tabletInfo->Owners.push_back(MakeWeak(tableInfo));

                    tabletInfo = TabletCache_.Insert(std::move(tabletInfo));
                    tableInfo->Tablets.push_back(tabletInfo);
                    if (tabletInfo->State == ETabletState::Mounted) {
                        tableInfo->MountedTablets.push_back(tabletInfo);
                    }
                }

                for (const auto& protoDescriptor : rsp->tablet_cells()) {
                    auto descriptor = FromProto<TCellDescriptor>(protoDescriptor);
                    CellDirectory_->ReconfigureCell(descriptor);
                }

                for (const auto& protoReplicaInfo : rsp->replicas()) {
                    auto replicaInfo = New<TTableReplicaInfo>();
                    replicaInfo->ReplicaId = FromProto<TTableReplicaId>(protoReplicaInfo.replica_id());
                    replicaInfo->ClusterName = protoReplicaInfo.cluster_name();
                    replicaInfo->ReplicaPath = protoReplicaInfo.replica_path();
                    replicaInfo->Mode = ETableReplicaMode(protoReplicaInfo.mode());
                    tableInfo->Replicas.push_back(replicaInfo);
                }

                if (tableInfo->IsSorted()) {
                    tableInfo->LowerCapBound = MinKey();
                    tableInfo->UpperCapBound = MaxKey();
                } else {
                    auto makeCapBound = [] (int tabletIndex) {
                        TUnversionedOwningRowBuilder builder;
                        builder.AddValue(MakeUnversionedInt64Value(tabletIndex));
                        return builder.FinishRow();
                    };
                    tableInfo->LowerCapBound = makeCapBound(0);
                    tableInfo->UpperCapBound = makeCapBound(static_cast<int>(tableInfo->Tablets.size()));
                }

                LOG_DEBUG("Table mount info received (Path: %v, TableId: %v, TabletCount: %v, Dynamic: %v)",
                    path,
                    tableInfo->TableId,
                    tableInfo->Tablets.size(),
                    tableInfo->Dynamic);

                return tableInfo;
            }));
    }

private:
    const TCellDirectoryPtr CellDirectory_;
    TObjectServiceProxy ObjectProxy_;
};

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    IChannelPtr masterChannel,
    TCellDirectoryPtr cellDirectory,
    const NLogging::TLogger& logger)
{
    return New<TTableMountCache>(
        std::move(config),
        std::move(masterChannel),
        std::move(cellDirectory),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

