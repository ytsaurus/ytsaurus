#include "native_table_mount_cache.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/table_mount_cache_detail.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/datetime/base.h>

namespace NYT::NTabletClient {

using namespace NApi;
using namespace NTracing;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NElection;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NApi::NNative;

using NNative::IConnection;
using NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTableMountCache)

class TTableMountCache
    : public TTableMountCacheBase
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        IConnectionPtr connection,
        ICellDirectoryPtr cellDirectory,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : TTableMountCacheBase(std::move(config), logger, profiler.WithPrefix("/table_mount_cache"))
        , Connection_(std::move(connection))
        , CellDirectory_(std::move(cellDirectory))
        , Invoker_(connection->GetInvoker())
        , TableMountInfoUpdateInvoker_(CreateSerializedInvoker(Invoker_))
    { }

private:
    TFuture<TTableMountInfoPtr> DoGet(
        const TYPath& /*key*/,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TTableMountInfoPtr> DoGet(
        const TYPath& key,
        const TErrorOr<TTableMountInfoPtr>* oldValue,
        EUpdateReason reason) noexcept override
    {
        NHydra::TRevision updatePrimaryRevision = NHydra::NullRevision;
        NHydra::TRevision updateSecondaryRevision = NHydra::NullRevision;

        if (reason == EUpdateReason::ForcedUpdate && oldValue && oldValue->IsOK()) {
            const auto& oldMountInfo = oldValue->Value();

            updatePrimaryRevision = oldMountInfo->PrimaryRevision;
            updateSecondaryRevision = oldMountInfo->SecondaryRevision;
        }

        std::optional<TTraceContextGuard> guard;

        if (reason == EUpdateReason::PeriodicUpdate) {
            guard.emplace(TTraceContext::NewRoot("PeriodicUpdate"));

            YT_LOG_DEBUG("Running periodic mount info update (TablePath: %v)", key);
        }

        auto session = New<TGetSession>(
            this,
            Connection_,
            key,
            updatePrimaryRevision,
            updateSecondaryRevision,
            reason == EUpdateReason::PeriodicUpdate,
            Logger);

        return BIND(&TGetSession::Run, std::move(session))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const ICellDirectoryPtr CellDirectory_;
    const IInvokerPtr Invoker_;
    const IInvokerPtr TableMountInfoUpdateInvoker_;

    class TGetSession
        : public TRefCounted
    {
    public:
        TGetSession(
            TTableMountCachePtr owner,
            TWeakPtr<IConnection> connection,
            const NYPath::TYPath& key,
            NHydra::TRevision refreshPrimaryRevision,
            NHydra::TRevision refreshSecondaryRevision,
            bool isPeriodicUpdate,
            const NLogging::TLogger& logger)
            : Owner_(std::move(owner))
            , Connection_(std::move(connection))
            , Path_(key)
            , RefreshPrimaryRevision_(refreshPrimaryRevision)
            , RefreshSecondaryRevision_(refreshSecondaryRevision)
            , IsPeriodicUpdate_(isPeriodicUpdate)
            , Logger(logger.WithTag("Path: %v, CacheSessionId: %v",
                Path_,
                TGuid::Create()))
        { }

        TTableMountInfoPtr Run()
        {
            try {
                WaitFor(RequestTableAttributes(RefreshPrimaryRevision_))
                    .ThrowOnError();

                auto mountInfoOrError = WaitFor(RequestMountInfo(RefreshSecondaryRevision_));
                if (!mountInfoOrError.IsOK() && PrimaryRevision_) {
                    WaitFor(RequestTableAttributes(PrimaryRevision_))
                        .ThrowOnError();
                    mountInfoOrError = WaitFor(RequestMountInfo(NHydra::NullRevision));
                }

                if (!mountInfoOrError.IsOK() && SecondaryRevision_) {
                    mountInfoOrError = WaitFor(RequestMountInfo(SecondaryRevision_));
                }

                return mountInfoOrError.ValueOrThrow();
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Error getting table mount info");
                THROW_ERROR_EXCEPTION("Error getting mount info for %v",
                    Path_)
                    << ex;
            }
        }

    private:
        const TTableMountCachePtr Owner_;
        const TWeakPtr<IConnection> Connection_;
        const NYPath::TYPath Path_;
        const NHydra::TRevision RefreshPrimaryRevision_;
        const NHydra::TRevision RefreshSecondaryRevision_;
        const bool IsPeriodicUpdate_;
        const NLogging::TLogger Logger;

        // PhysicalPath points to a physical object, if current object is linked to some other object, then this field will point to the source.
        NYPath::TYPath PhysicalPath_;
        TTableId TableId_;
        TCellTag CellTag_;
        NHydra::TRevision PrimaryRevision_ = NHydra::NullRevision;
        NHydra::TRevision SecondaryRevision_ = NHydra::NullRevision;

        TMasterReadOptions GetMasterReadOptions()
        {
            auto cacheConfig = Owner_->GetConfig();
            auto refreshTime = cacheConfig->RefreshTime.value_or(TDuration::Max());
            return {
                .ReadFrom = EMasterChannelKind::Cache,
                .ExpireAfterSuccessfulUpdateTime = Min(cacheConfig->ExpireAfterSuccessfulUpdateTime, refreshTime),
                .ExpireAfterFailedUpdateTime = Min(cacheConfig->ExpireAfterFailedUpdateTime, refreshTime),
                .EnableClientCacheStickiness = true
            };
        }

        TFuture<void> RequestTableAttributes(NHydra::TRevision refreshPrimaryRevision)
        {
            auto connection = Connection_.Lock();
            if (!connection) {
                return MakeFuture<void>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
            }

            YT_LOG_DEBUG("Requesting table mount info from primary master (RefreshPrimaryRevision: %x)",
                refreshPrimaryRevision);

            auto options = GetMasterReadOptions();

            auto primaryProxy = TObjectServiceProxy(
                connection,
                options.ReadFrom,
                PrimaryMasterCellTagSentinel,
                connection->GetStickyGroupSizeCache(),
                NRpc::TAuthenticationIdentity(NSecurityClient::TableMountInformerUserName));
            auto batchReq = primaryProxy.ExecuteBatch();
            SetBalancingHeader(batchReq, connection, options);

            {
                auto req = TTableYPathProxy::Get(Path_ + "/@");
                ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                    "id",
                    "dynamic",
                    "external_cell_tag",
                    "path",
                });

                SetCachingHeader(req, connection, options, refreshPrimaryRevision);

                size_t hash = 0;
                HashCombine(hash, FarmHash(Path_.begin(), Path_.size()));
                batchReq->AddRequest(req, "get_attributes", hash);
            }

            return batchReq->Invoke()
                .Apply(BIND(&TGetSession::OnTableAttributesReceived, MakeWeak(this)));
        }

        void OnTableAttributesReceived(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of table %v",
                Path_);

            const auto& batchRsp = batchRspOrError.Value();
            auto getAttributesRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            auto& rsp = getAttributesRspOrError.Value();

            PrimaryRevision_ = batchRsp->GetRevision(0);

            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

            PhysicalPath_ = attributes->Get<NYPath::TYPath>("path", Path_);
            CellTag_ = attributes->Get<TCellTag>("external_cell_tag", PrimaryMasterCellTagSentinel);
            TableId_ = attributes->Get<TObjectId>("id");
            auto dynamic = attributes->Get<bool>("dynamic", false);

            auto type = TypeFromId(TableId_);
            if (IsTableType(type) && !dynamic) {
                THROW_ERROR_EXCEPTION(
                    "Table %v is not dynamic",
                    Path_);
            }
        }

        TFuture<TTableMountInfoPtr> RequestMountInfo(NHydra::TRevision refreshSecondaryRevision)
        {
            auto connection = Connection_.Lock();
            if (!connection) {
                return MakeFuture<TTableMountInfoPtr>(TError(NYT::EErrorCode::Canceled, "Connection destroyed"));
            }

            YT_LOG_DEBUG("Requesting table mount info from secondary master (TableId: %v, CellTag: %v, RefreshSecondaryRevision: %x)",
                TableId_,
                CellTag_,
                refreshSecondaryRevision);

            auto options = GetMasterReadOptions();

            auto secondaryProxy = TObjectServiceProxy(
                connection,
                options.ReadFrom,
                CellTag_,
                connection->GetStickyGroupSizeCache(),
                NRpc::TAuthenticationIdentity(NSecurityClient::TableMountInformerUserName));
            auto batchReq = secondaryProxy.ExecuteBatch();
            SetBalancingHeader(batchReq, connection, options);

            auto req = TTableYPathProxy::GetMountInfo(FromObjectId(TableId_));
            SetCachingHeader(req, connection, options, refreshSecondaryRevision);

            size_t hash = 0;
            HashCombine(hash, FarmHash(TableId_.Parts64[0]));
            HashCombine(hash, FarmHash(TableId_.Parts64[1]));
            batchReq->AddRequest(req, std::nullopt, hash);

            auto responseHandler = BIND(&TGetSession::OnTableMountInfoReceived, MakeStrong(this));
            if (IsPeriodicUpdate_) {
                // For background updates we serialize table mount info processing via #TableMountInfoUpdateInvoker to reduce
                // contention on the #TableInfoCache and avoid concurrent processing of heavy responses.
                return batchReq->Invoke()
                    .Apply(responseHandler
                        .AsyncVia(Owner_->TableMountInfoUpdateInvoker_));
            } else {
                return batchReq->Invoke()
                    .Apply(responseHandler);
            }
        }

        TTableMountInfoPtr OnTableMountInfoReceived(const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
        {
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting mount info for table %v",
                Path_);

            const auto& batchRsp = batchRspOrError.Value();
            const auto& rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspGetMountInfo>(0);
            const auto& rsp = rspOrError.Value();

            SecondaryRevision_ = batchRsp->GetRevision(0);

            auto tableInfo = New<TTableMountInfo>();
            tableInfo->Path = Path_;
            tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
            tableInfo->SecondaryRevision = SecondaryRevision_;
            tableInfo->PrimaryRevision = PrimaryRevision_;
            tableInfo->HunkStorageId = FromProto<TObjectId>(rsp->hunk_storage_node_id());

            auto primarySchema = FromProto<TTableSchemaPtr>(rsp->schema());
            tableInfo->Schemas[ETableSchemaKind::Primary] = primarySchema;
            tableInfo->Schemas[ETableSchemaKind::Write] = primarySchema->ToWrite();
            tableInfo->Schemas[ETableSchemaKind::WriteViaQueueProducer] = primarySchema->ToWriteViaQueueProducer();
            tableInfo->Schemas[ETableSchemaKind::VersionedWrite] = primarySchema->ToVersionedWrite();
            tableInfo->Schemas[ETableSchemaKind::Delete] = primarySchema->ToDelete();
            tableInfo->Schemas[ETableSchemaKind::Query] = primarySchema->ToQuery();
            tableInfo->Schemas[ETableSchemaKind::Lookup] = primarySchema->ToLookup();
            tableInfo->Schemas[ETableSchemaKind::Lock] = primarySchema->ToLock();
            tableInfo->Schemas[ETableSchemaKind::PrimaryWithTabletIndex] = primarySchema->WithTabletIndex();
            tableInfo->Schemas[ETableSchemaKind::ReplicationLog] = primarySchema->ToReplicationLog();

            tableInfo->UpstreamReplicaId = FromProto<TTableReplicaId>(rsp->upstream_replica_id());
            tableInfo->Dynamic = rsp->dynamic();
            tableInfo->NeedKeyEvaluation = primarySchema->HasComputedColumns();
            tableInfo->EnableDetailedProfiling = rsp->enable_detailed_profiling();
            tableInfo->ReplicationCardId = FromProto<TReplicationCardId>(rsp->replication_card_id());
            tableInfo->PhysicalPath = PhysicalPath_;

            for (const auto& protoTabletInfo : rsp->tablets()) {
                auto tabletInfo = New<TTabletInfo>();
                tabletInfo->CellId = FromProto<TCellId>(protoTabletInfo.cell_id());
                tabletInfo->TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
                tabletInfo->MountRevision = protoTabletInfo.mount_revision();
                tabletInfo->State = FromProto<ETabletState>(protoTabletInfo.state());
                tabletInfo->UpdateTime = Now();
                tabletInfo->InMemoryMode = FromProto<EInMemoryMode>(protoTabletInfo.in_memory_mode());

                if (tableInfo->IsSorted()) {
                    // Take the actual pivot from master response.
                    tabletInfo->PivotKey = FromProto<TLegacyOwningKey>(protoTabletInfo.pivot_key());
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

                Owner_->TabletInfoOwnerCache_.Insert(tabletInfo->TabletId, MakeWeak(tableInfo));

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
                replicaInfo->Mode = FromProto<ETableReplicaMode>(protoReplicaInfo.mode());
                tableInfo->Replicas.push_back(replicaInfo);
            }

            tableInfo->Indices.reserve(rsp->indices_size());
            for (const auto& protoIndexInfo : rsp->indices()) {
                TIndexInfo indexInfo{
                    .TableId = FromProto<TObjectId>(protoIndexInfo.index_table_id()),
                    .Kind = FromProto<ESecondaryIndexKind>(protoIndexInfo.index_kind()),
                    .Predicate = YT_PROTO_OPTIONAL(protoIndexInfo, predicate),
                };
                tableInfo->Indices.push_back(indexInfo);
            }

            if (tableInfo->IsSorted()) {
                tableInfo->LowerCapBound = MinKey();
                tableInfo->UpperCapBound = MaxKey();
            } else {
                tableInfo->LowerCapBound = MakeUnversionedOwningRow(static_cast<int>(0));
                tableInfo->UpperCapBound = MakeUnversionedOwningRow(static_cast<int>(tableInfo->Tablets.size()));
            }

            YT_LOG_DEBUG("Table mount info received (TableId: %v, TabletCount: %v, Dynamic: %v, PrimaryRevision: %x, SecondaryRevision: %x)",
                tableInfo->TableId,
                tableInfo->Tablets.size(),
                tableInfo->Dynamic,
                tableInfo->PrimaryRevision,
                tableInfo->SecondaryRevision);

            return tableInfo;
        }
    };

    void InvalidateTable(const TTableMountInfoPtr& tableInfo) override
    {
        ForceRefresh(tableInfo->Path, tableInfo);
    }

    void OnAdded(const TYPath& key) noexcept override
    {
        YT_LOG_DEBUG("Table mount info added to cache (Path: %v)", key);
    }

    void OnRemoved(const TYPath& key) noexcept override
    {
        YT_LOG_DEBUG("Table mount info removed from cache (Path: %v)", key);
    }

    void RegisterCell(INodePtr cellDescriptor) override
    {
        auto nativeCellDescriptor = ConvertTo<TCellDescriptor>(cellDescriptor);
        CellDirectory_->ReconfigureCell(nativeCellDescriptor);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountCache)

////////////////////////////////////////////////////////////////////////////////

ITableMountCachePtr CreateNativeTableMountCache(
    TTableMountCacheConfigPtr config,
    IConnectionPtr connection,
    ICellDirectoryPtr cellDirectory,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TTableMountCache>(
        std::move(config),
        std::move(connection),
        std::move(cellDirectory),
        logger,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

