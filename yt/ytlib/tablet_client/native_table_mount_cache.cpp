#include "table_mount_cache.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/query_client/query_statistics.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/async_expiring_cache.h>
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

class TTabletCache
{
public:
    TTabletInfoPtr Find(const TTabletId& tabletId)
    {
        TReaderGuard guard(SpinLock_);
        RemoveExpiredEntries();
        auto it = Map_.find(tabletId);
        return it != Map_.end() ? it->second.Lock() : nullptr;
    }

    TTabletInfoPtr Insert(TTabletInfoPtr tabletInfo)
    {
        TWriterGuard guard(SpinLock_);
        auto it = Map_.find(tabletInfo->TabletId);
        if (it != Map_.end()) {
            if (auto existingTabletInfo = it->second.Lock()) {
                if (tabletInfo->MountRevision < existingTabletInfo->MountRevision) {
                    THROW_ERROR_EXCEPTION("Tablet mount revision %v is outdated",
                        tabletInfo->MountRevision);
                }

                for (const auto& owner : existingTabletInfo->Owners) {
                    if (!owner.IsExpired()) {
                        tabletInfo->Owners.push_back(owner);
                    }
                }
            }

            it->second = MakeWeak(tabletInfo);
        } else {
            YCHECK(Map_.insert({tabletInfo->TabletId, tabletInfo}).second);
        }
        return tabletInfo;
    }

private:
    THashMap<TTabletId, TWeakPtr<TTabletInfo>> Map_;
    TReaderWriterSpinLock SpinLock_;
    TInstant LastExpiredRemovalTime_;
    static const TDuration ExpiringTimeout_;

    void RemoveExpiredEntries()
    {
        if (LastExpiredRemovalTime_ + ExpiringTimeout_ < Now()) {
            return;
        }

        std::vector<TTabletId> removeIds;
        for (auto it = Map_.begin(); it != Map_.end(); ++it) {
            if (it->second.IsExpired()) {
                removeIds.push_back(it->first);
            }
        }
        for (const auto& tabletId : removeIds) {
            Map_.erase(tabletId);
        }

        LastExpiredRemovalTime_ = Now();
    }
};

const TDuration TTabletCache::ExpiringTimeout_ = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

bool TTableMountInfo::IsSorted() const
{
    return Schemas[ETableSchemaKind::Primary].IsSorted();
}

bool TTableMountInfo::IsOrdered() const
{
    return !IsSorted();
}

bool TTableMountInfo::IsReplicated() const
{
    return TypeFromId(TableId) == EObjectType::ReplicatedTable;
}

TTabletInfoPtr TTableMountInfo::GetTabletByIndexOrThrow(int tabletIndex) const
{
    if (tabletIndex < 0 || tabletIndex >= Tablets.size()) {
        THROW_ERROR_EXCEPTION("Invalid tablet index: expected in range [0,%v], got %v",
            Tablets.size() - 1,
            tabletIndex);
    }
    return Tablets[tabletIndex];
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(const TRange<TUnversionedValue>& row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.Size() >= keyColumnCount);
    ValidateDynamic();
    auto it = std::upper_bound(
        Tablets.begin(),
        Tablets.end(),
        row,
        [&] (const TRange<TUnversionedValue>& key, const TTabletInfoPtr& rhs) {
            return CompareRows(
                key.Begin(),
                key.Begin() + keyColumnCount,
                rhs->PivotKey.Begin(),
                rhs->PivotKey.End()) < 0;
        });
    YCHECK(it != Tablets.begin());
    return *(--it);
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TUnversionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.GetCount() >= keyColumnCount);
    return GetTabletForRow(MakeRange(row.Begin(), row.Begin() + keyColumnCount));
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TVersionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary].GetKeyColumnCount();
    YCHECK(row.GetKeyCount() == keyColumnCount);
    return GetTabletForRow(MakeRange(row.BeginKeys(), row.EndKeys()));
}

TTabletInfoPtr TTableMountInfo::GetRandomMountedTablet() const
{
    ValidateDynamic();

    if (MountedTablets.empty()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::TabletNotMounted,
            "Table %v has no mounted tablets",
            Path);
    }

    size_t index = RandomNumber(MountedTablets.size());
    return MountedTablets[index];
}

void TTableMountInfo::ValidateDynamic() const
{
    if (!Dynamic) {
        THROW_ERROR_EXCEPTION("Table %v is not dynamic", Path);
    }
}

void TTableMountInfo::ValidateSorted() const
{
    if (!IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted", Path);
    }
}

void TTableMountInfo::ValidateOrdered() const
{
    if (!IsOrdered()) {
        THROW_ERROR_EXCEPTION("Table %v is not ordered", Path);
    }
}

void TTableMountInfo::ValidateNotReplicated() const
{
    if (IsReplicated()) {
        THROW_ERROR_EXCEPTION("Table %v is replicated", Path);
    }
}

void TTableMountInfo::ValidateReplicated() const
{
    if (!IsReplicated()) {
        THROW_ERROR_EXCEPTION("Table %v is not replicated", Path);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache
    : public ITableMountCache
    , public TExpiringCache<TYPath, TTableMountInfoPtr>
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel,
        TCellDirectoryPtr cellDirectory,
        const NLogging::TLogger& logger)
        : TExpiringCache(config)
        , Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , Logger(logger)
        , ObjectProxy_(masterChannel)
    { }

    virtual TFuture<TTableMountInfoPtr> GetTableInfo(const TYPath& path) override
    {
        return TExpiringCache::Get(path);
    }

    virtual TTabletInfoPtr FindTablet(const TTabletId& tabletId) override
    {
        return TabletCache_.Find(tabletId);
    }

    virtual void InvalidateTablet(TTabletInfoPtr tabletInfo) override
    {
        for (const auto& weakOwner : tabletInfo->Owners) {
            if (auto owner = weakOwner.Lock()) {
                Invalidate(owner->Path);
            }
        }
    }

    virtual std::pair<bool, TTabletInfoPtr> InvalidateOnError(const TError& error) override
    {
        static std::vector<NTabletClient::EErrorCode> retriableCodes = {
            NTabletClient::EErrorCode::NoSuchTablet,
            NTabletClient::EErrorCode::TabletNotMounted,
            NTabletClient::EErrorCode::InvalidMountRevision
        };

        if (!error.IsOK()) {
            for (auto errCode : retriableCodes) {
                if (auto retriableError = error.FindMatching(errCode)) {
                    // COMPAT(savrus) Not all above exceptions had tablet_id attribute in early 19.2 versions.
                    auto tabletId = retriableError->Attributes().Find<TTabletId>("tablet_id");
                    if (!tabletId) {
                        continue;
                    }
                    auto tabletInfo = FindTablet(*tabletId);
                    if (tabletInfo) {
                        LOG_DEBUG(error, "Invalidating tablet in table mount cache (TabletId: %v, Owners:%v)",
                            tabletInfo->TabletId,
                            MakeFormattableRange(tabletInfo->Owners, [] (TStringBuilder* builder, const TWeakPtr<TTableMountInfo>& weakOwner) {
                                if (auto owner = weakOwner.Lock()) {
                                    FormatValue(builder, owner->Path, TStringBuf());
                                }
                            }));

                        LOG_DEBUG(error, "Invalidating tablet in table mount cache (TabletId: %v)", *tabletId);
                        InvalidateTablet(tabletInfo);
                    }
                    return std::make_pair(true, tabletInfo);
                }
            }
        }

        return std::make_pair(false, nullptr);
    }

    virtual void Clear()
    {
        TExpiringCache::Clear();
        LOG_DEBUG("Table mount info cache cleared");
    }

private:
    const TTableMountCacheConfigPtr Config_;
    const TCellDirectoryPtr CellDirectory_;

    const NLogging::TLogger Logger;

    TObjectServiceProxy ObjectProxy_;
    TTabletCache TabletCache_;

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

