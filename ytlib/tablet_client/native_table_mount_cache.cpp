#include "native_table_mount_cache.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/client/tablet_client/table_mount_cache.h>
#include <yt/client/tablet_client/table_mount_cache_detail.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/farm_hash.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/ytree/proto/ypath.pb.h>

#include <yt/core/yson/string.h>

#include <util/datetime/base.h>

namespace NYT::NTabletClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NElection;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NNative::IConnection;
using NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache
    : public TTableMountCacheBase
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        IConnectionPtr connection,
        TCellDirectoryPtr cellDirectory,
        const NLogging::TLogger& logger)
        : TTableMountCacheBase(std::move(config), logger)
        , Connection_(std::move(connection))
        , CellDirectory_(std::move(cellDirectory))
    { }

private:

    virtual TFuture<TTableMountInfoPtr> DoGet(const TTableMountCacheKey& key) override
    {
        auto connection = Connection_.Lock();
        if (!connection) {
            auto error = TError("Unable to get table mount info: сonnection terminated")
                << TErrorAttribute("table_path", key.Path);
            return MakeFuture<TTableMountInfoPtr>(error);
        }

        auto invoker = connection->GetInvoker();
        auto session = New<TGetSession>(this, std::move(connection), key, Logger);

        return BIND(&TGetSession::Run, std::move(session))
            .AsyncVia(std::move(invoker))
            .Run();
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const TCellDirectoryPtr CellDirectory_;

    class TGetSession
        : public TRefCounted
    {
    public:
        TGetSession(
            TTableMountCache* owner,
            IConnectionPtr connection,
            const TTableMountCacheKey& key,
            const NLogging::TLogger& logger)
            : Owner_(owner)
            , Connection_(std::move(connection))
            , Key_(key)
            , Logger(logger)
        {
            Logger.AddTag("Path: %v, CacheSessionId: %v",
                Key_.Path,
                TGuid::Create());
        }

        TTableMountInfoPtr Run()
        {
            WaitFor(RequestTableAttributes(Key_.RefreshPrimaryRevision))
                .ThrowOnError();
            auto mountInfoOrError = WaitFor(RequestMountInfo(Key_.RefreshSecondaryRevision));

            if (!mountInfoOrError.IsOK() && PrimaryRevision_) {
                WaitFor(RequestTableAttributes(PrimaryRevision_))
                    .ThrowOnError();
                mountInfoOrError = WaitFor(RequestMountInfo(std::nullopt));
            }

            if (!mountInfoOrError.IsOK() && SecondaryRevision_) {
                mountInfoOrError = WaitFor(RequestMountInfo(SecondaryRevision_));
            }

            if (!mountInfoOrError.IsOK()) {
                auto wrappedError = TError("Error getting mount info for %v",
                    Key_.Path)
                    << mountInfoOrError;
                YT_LOG_WARNING(wrappedError);
                THROW_ERROR wrappedError;
            }

            return mountInfoOrError.Value();
        }

    private:
        const TIntrusivePtr<TTableMountCache> Owner_;
        const IConnectionPtr Connection_;
        const TTableMountCacheKey Key_;

        TTableId TableId_;
        TCellTag CellTag_;
        std::optional<i64> PrimaryRevision_;
        std::optional<i64> SecondaryRevision_;

        NLogging::TLogger Logger;

        TFuture<void> RequestTableAttributes(std::optional<i64> refreshPrimaryRevision)
        {
            YT_LOG_DEBUG("Requesting table mount info from primary master (RefreshPrimaryRevision: %v)",
                refreshPrimaryRevision);

            auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Cache);
            auto primaryProxy = TObjectServiceProxy(channel);
            auto batchReq = primaryProxy.ExecuteBatch();

            auto* balancingHeaderExt = batchReq->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
            balancingHeaderExt->set_enable_stickness(true);
            balancingHeaderExt->set_sticky_group_size(1);

            {
                auto req = TTableYPathProxy::Get(Key_.Path + "/@");
                std::vector<TString> attributeKeys{
                    "id",
                    "dynamic",
                    "external_cell_tag"
                };
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

                auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
                cachingHeaderExt->set_success_expiration_time(ToProto<i64>(Owner_->Config_->ExpireAfterSuccessfulUpdateTime));
                cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(Owner_->Config_->ExpireAfterFailedUpdateTime));
                if (refreshPrimaryRevision) {
                    cachingHeaderExt->set_refresh_revision(*refreshPrimaryRevision);
                }

                size_t hash = 0;
                HashCombine(hash, FarmHash(Key_.Path.begin(), Key_.Path.size()));
                batchReq->AddRequest(req, "get_attributes", hash);
            }

            return batchReq->Invoke()
                .Apply(BIND(&TGetSession::OnTableAttributesReceived, MakeWeak(this)));
        }

        void OnTableAttributesReceived(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attriubtes of table %v", Key_.Path);
            const auto& batchRsp = batchRspOrError.Value();
            auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            auto& rsp = getAttributesRspOrError.Value();

            PrimaryRevision_ = batchRsp->GetRevision(0);

            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            CellTag_ = attributes->Get<TCellTag>("external_cell_tag", PrimaryMasterCellTag);
            TableId_ = attributes->Get<TObjectId>("id");
            auto dynamic = attributes->Get<bool>("dynamic", false);

            if (!dynamic) {
                THROW_ERROR_EXCEPTION("Table %v is not dynamic",
                    Key_.Path);
            }
        }

        TFuture<TTableMountInfoPtr> RequestMountInfo(std::optional<i64> refreshSecondaryRevision)
        {
            YT_LOG_DEBUG("Requesting table mount info from secondary master (TableId: %v, CellTag: %v, RefreshSecondaryRevision: %v)",
                TableId_,
                CellTag_,
                refreshSecondaryRevision);

            auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Cache, CellTag_);
            auto secondaryProxy = TObjectServiceProxy(channel);
            auto batchReq = secondaryProxy.ExecuteBatch();

            auto* balancingHeaderExt = batchReq->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
            balancingHeaderExt->set_enable_stickness(true);
            balancingHeaderExt->set_sticky_group_size(1);

            {
                auto req = TTableYPathProxy::GetMountInfo(FromObjectId(TableId_));

                auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
                cachingHeaderExt->set_success_expiration_time(ToProto<i64>(Owner_->Config_->ExpireAfterSuccessfulUpdateTime));
                cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(Owner_->Config_->ExpireAfterFailedUpdateTime));
                if (refreshSecondaryRevision) {
                    cachingHeaderExt->set_refresh_revision(*refreshSecondaryRevision);
                }

                size_t hash = 0;
                HashCombine(hash, FarmHash(TableId_.Parts64[0]));
                HashCombine(hash, FarmHash(TableId_.Parts64[1]));
                batchReq->AddRequest(req, "get_mount_info", hash);
            }

            return batchReq->Invoke()
                .Apply(BIND(&TGetSession::OnTableMountInfoReceived, MakeStrong(this)));
        }

        TTableMountInfoPtr OnTableMountInfoReceived (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting mount info for table %v", Key_.Path);

            const auto& batchRsp = batchRspOrError.Value();
            const auto& rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspGetMountInfo>(0);
            const auto& rsp = rspOrError.Value();

            SecondaryRevision_ = batchRsp->GetRevision(0);

            auto tableInfo = New<TTableMountInfo>();
            tableInfo->Path = Key_.Path;
            tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
            tableInfo->SecondaryRevision = SecondaryRevision_;
            tableInfo->PrimaryRevision = PrimaryRevision_;

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
                    ? std::make_optional(EInMemoryMode(protoTabletInfo.in_memory_mode()))
                    : std::nullopt;

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

                tabletInfo = Owner_->TabletCache_.Insert(std::move(tabletInfo));
                tableInfo->Tablets.push_back(tabletInfo);
                if (tabletInfo->State == ETabletState::Mounted) {
                    tableInfo->MountedTablets.push_back(tabletInfo);
                }
            }

            for (const auto& protoDescriptor : rsp->tablet_cells()) {
                auto descriptor = FromProto<TCellDescriptor>(protoDescriptor);
                Owner_->CellDirectory_->ReconfigureCell(descriptor);
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

            YT_LOG_DEBUG("Table mount info received (TableId: %v, TabletCount: %v, Dynamic: %v, PrimaryRevision: %v, SecondaryRevision: %v)",
                tableInfo->TableId,
                tableInfo->Tablets.size(),
                tableInfo->Dynamic,
                tableInfo->PrimaryRevision,
                tableInfo->SecondaryRevision);

            return tableInfo;
        }
    };

    virtual void InvalidateTable(const TTableMountInfoPtr& tableInfo) override
    {
        Invalidate(tableInfo->Path);

        TAsyncExpiringCache::Get(TTableMountCacheKey{
            tableInfo->Path,
            tableInfo->PrimaryRevision,
            tableInfo->SecondaryRevision});
    }
};

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    IConnectionPtr connection,
    TCellDirectoryPtr cellDirectory,
    const NLogging::TLogger& logger)
{
    return New<TTableMountCache>(
        std::move(config),
        std::move(connection),
        std::move(cellDirectory),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

