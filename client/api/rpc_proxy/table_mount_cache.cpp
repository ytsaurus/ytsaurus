#include "table_mount_cache.h"
#include "api_service_proxy.h"
#include "helpers.h"

#include <yt/client/api/config.h>

#include <yt/client/tablet_client/config.h>

#include <yt/client/tablet_client/table_mount_cache_detail.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;

///////////////////////////////////////////////////////////////////////////////

class TTableMountCache
    : public TTableMountCacheBase
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        IChannelPtr channel,
        const NLogging::TLogger& logger,
        TDuration timeout)
        : TTableMountCacheBase(std::move(config), logger)
        , Channel_(std::move(channel))
        , Timeout_(timeout)
    { }

private:
    virtual TFuture<TTableMountInfoPtr> DoGet(const TTableMountCacheKey& key) override
    {
        const auto& path = key.Path;

        LOG_DEBUG("Requesting table mount info (Path: %v)", path);

        TApiServiceProxy proxy(Channel_);
        proxy.SetDefaultTimeout(Timeout_);
        auto req = proxy.GetTableMountInfo();
        req->set_path(path);

        return req->Invoke().Apply(
            BIND([= , this_ = MakeStrong(this)] (const TApiServiceProxy::TRspGetTableMountInfoPtr& rsp) {
                auto tableInfo = New<TTableMountInfo>();
                tableInfo->Path = path;
                auto tableId = FromProto<NObjectClient::TObjectId>(rsp->table_id());
                tableInfo->TableId = tableId;

                auto& primarySchema = tableInfo->Schemas[ETableSchemaKind::Primary];
                primarySchema = FromProto<NTableClient::TTableSchema>(rsp->schema());

                tableInfo->Schemas[ETableSchemaKind::Write] = primarySchema.ToWrite();
                tableInfo->Schemas[ETableSchemaKind::VersionedWrite] = primarySchema.ToVersionedWrite();
                tableInfo->Schemas[ETableSchemaKind::Delete] = primarySchema.ToDelete();
                tableInfo->Schemas[ETableSchemaKind::Query] = primarySchema.ToQuery();
                tableInfo->Schemas[ETableSchemaKind::Lookup] = primarySchema.ToLookup();

                tableInfo->UpstreamReplicaId = FromProto<TTableReplicaId>(rsp->upstream_replica_id());
                tableInfo->Dynamic = rsp->dynamic();
                tableInfo->NeedKeyEvaluation = primarySchema.HasComputedColumns();

                for (const auto& protoTabletInfo : rsp->tablets()) {
                    auto tabletInfo = New<NTabletClient::TTabletInfo>();
                    FromProto(tabletInfo.Get(), protoTabletInfo);
                    tabletInfo->TableId = tableId;
                    tabletInfo->UpdateTime = Now();
                    tabletInfo->Owners.push_back(MakeWeak(tableInfo));

                    tabletInfo = TabletCache_.Insert(std::move(tabletInfo));
                    tableInfo->Tablets.push_back(tabletInfo);
                    if (tabletInfo->State == ETabletState::Mounted) {
                        tableInfo->MountedTablets.push_back(tabletInfo);
                    }
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
    const IChannelPtr Channel_;
    const TDuration Timeout_;

    virtual void InvalidateTable(const TTableMountInfoPtr& tableInfo) override
    {
        Invalidate(tableInfo->Path);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITableMountCachePtr CreateTableMountCache(
    TTableMountCacheConfigPtr config,
    IChannelPtr channel,
    const NLogging::TLogger& logger,
    TDuration timeout)
{
    return New<TTableMountCache>(
        std::move(config),
        std::move(channel),
        logger,
        timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
