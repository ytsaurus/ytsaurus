#include "stdafx.h"
#include "table_mount_cache.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/concurrency/rw_spinlock.h>
#include <core/concurrency/delayed_executor.h>

#include <core/ytree/ypath.pb.h>

#include <ytlib/election/config.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTabletClient {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NYPath;
using namespace NRpc;
using namespace NElection;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NHive;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTabletReplica::TTabletReplica()
    : Id(InvalidNodeId)
{ }

TTabletReplica::TTabletReplica(
    NNodeTrackerClient::TNodeId id,
    const TNodeDescriptor& descriptor)
    : Id(id)
    , Descriptor(descriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

TTableMountInfo::TTableMountInfo()
    : Sorted(false)
{ }

TTabletInfoPtr TTableMountInfo::GetTablet(TUnversionedRow row)
{
    if (Tablets.empty()) {
        THROW_ERROR_EXCEPTION("Table %v has no tablets",
            Path);
    }
    auto it = std::upper_bound(
        Tablets.begin(),
        Tablets.end(),
        row,
        [&] (TUnversionedRow lhs, const TTabletInfoPtr& rhs) {
            return CompareRows(lhs, rhs->PivotKey.Get(), KeyColumns.size()) < 0;
        });
    return *(it - 1);
}

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel,
        TCellDirectoryPtr cellDirectory)
        : Config_(config)
        , ObjectProxy_(masterChannel)
        , CellDirectory_(cellDirectory)
    { }

    TFuture<TErrorOr<TTableMountInfoPtr>> GetTableInfo(const TYPath& path)
    {
        auto now = TInstant::Now();

        // Fast path.
        {
            TReaderGuard guard(SpinLock_);
            auto it = PathToEntry_.find(path);
            if (it != PathToEntry_.end()) {
                const auto& entry = it->second;
                if (now < entry.Deadline) {
                    return entry.Promise;
                }
            }
        }

        // Slow path.
        {
            TWriterGuard guard(SpinLock_);
            auto it = PathToEntry_.find(path);
            if (it == PathToEntry_.end()) {
                TTableEntry entry;
                entry.Deadline = TInstant::Max();
                auto promise = entry.Promise = NewPromise<TErrorOr<TTableMountInfoPtr>>();
                YCHECK(PathToEntry_.insert(std::make_pair(path, entry)).second);
                guard.Release();
                RequestTableMountInfo(path);
                return promise;
            }

            auto& entry = it->second;
            const auto& promise = entry.Promise;
            if (!promise.IsSet()) {
                return promise;
            }

            if (now > entry.Deadline) {
                // Evict and retry.
                TDelayedExecutor::CancelAndClear(entry.ProbationCookie);
                PathToEntry_.erase(it);
                guard.Release();
                return GetTableInfo(path);
            }

            return promise;
        }
    }

    void Clear()
    {
        TWriterGuard guard(SpinLock_);
        PathToEntry_.clear();
        LOG_DEBUG("Table mount info cache cleared");
    }

private:
    TTableMountCacheConfigPtr Config_;
    TObjectServiceProxy ObjectProxy_;
    TCellDirectoryPtr CellDirectory_;
    

    struct TTableEntry
    {
        //! When this entry must be evicted.
        TInstant Deadline;
        //! Some latest known info (possibly not yet set).
        TPromise<TErrorOr<TTableMountInfoPtr>> Promise;
        //! Corresponds to a future probation request.
        TDelayedExecutorCookie ProbationCookie;
    };

    TReaderWriterSpinLock SpinLock_;
    yhash<TYPath, TTableEntry> PathToEntry_;


    void RequestTableMountInfo(const TYPath& path)
    {
        LOG_DEBUG("Requesting table mount info for %v",
            path);

        auto req = TTableYPathProxy::GetMountInfo(path);
        auto* cachingHeaderExt = req->Header().MutableExtension(TCachingHeaderExt::caching_header_ext);
        cachingHeaderExt->set_success_expiration_time(Config_->SuccessExpirationTime.MilliSeconds());
        cachingHeaderExt->set_failure_expiration_time(Config_->FailureExpirationTime.MilliSeconds());

        ObjectProxy_.Execute(req).Subscribe(
            BIND(&TImpl::OnTableMountInfoResponse, MakeWeak(this), path));
    }

    void OnTableMountInfoResponse(const TYPath& path, TTableYPathProxy::TRspGetMountInfoPtr rsp)
    {
        TWriterGuard guard(SpinLock_);
        auto it = PathToEntry_.find(path);
        if (it == PathToEntry_.end())
            return;

        auto& entry = it->second;

        auto setResult = [&] (const TErrorOr<TTableMountInfoPtr>& result) {
            auto expirationTime = result.IsOK() ? Config_->SuccessExpirationTime : Config_->FailureExpirationTime;
            entry.Deadline = TInstant::Now() + expirationTime;
            if (entry.Promise.IsSet()) {
                entry.Promise = MakePromise(result);
            } else {
                entry.Promise.Set(result);
            }
        };

        if (!rsp->IsOK()) {
            auto error = TError("Error getting mount info for %v",
                path)
                << *rsp;
            setResult(error);
            LOG_WARNING(error);
            return;
        }

        auto tableInfo = New<TTableMountInfo>();
        tableInfo->Path = path;
        tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
        tableInfo->Schema = FromProto<TTableSchema>(rsp->schema());
        tableInfo->KeyColumns = FromProto<TKeyColumns>(rsp->key_columns());
        tableInfo->Sorted = rsp->sorted();

        auto nodeDirectory = New<TNodeDirectory>();
        nodeDirectory->MergeFrom(rsp->node_directory());

        for (const auto& protoTabletInfo : rsp->tablets()) {
            auto tabletInfo = New<TTabletInfo>();
            tabletInfo->TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
            tabletInfo->State = ETabletState(protoTabletInfo.state());
            tabletInfo->PivotKey = FromProto<TOwningKey>(protoTabletInfo.pivot_key());

            if (protoTabletInfo.has_cell_config()) {
                auto config = ConvertTo<TCellConfigPtr>(TYsonString(protoTabletInfo.cell_config()));
                tabletInfo->CellId = config->CellId;
                CellDirectory_->RegisterCell(config, protoTabletInfo.cell_config_version());
            }
            
            for (auto nodeId : protoTabletInfo.replica_node_ids()) {
                tabletInfo->Replicas.push_back(TTabletReplica(
                    nodeId,
                    nodeDirectory->GetDescriptor(nodeId)));
            }

            tableInfo->Tablets.push_back(tabletInfo);
        }

        setResult(tableInfo);

        entry.ProbationCookie = TDelayedExecutor::Submit(
            BIND(&TImpl::RequestTableMountInfo, MakeWeak(this), path),
            Config_->SuccessProbationTime);

        LOG_DEBUG("Table mount info received (Path: %v, TableId: %v, TabletCount: %v, Sorted: %lv)",
            path,
            tableInfo->TableId,
            tableInfo->Tablets.size(),
            tableInfo->Sorted);
    }

};

////////////////////////////////////////////////////////////////////////////////

TTableMountCache::TTableMountCache(
    TTableMountCacheConfigPtr config,
    IChannelPtr masterChannel,
    TCellDirectoryPtr cellDirectory)
    : Impl_(New<TImpl>(
        config,
        masterChannel,
        cellDirectory))
{ }

TTableMountCache::~TTableMountCache()
{ }

TFuture<TErrorOr<TTableMountInfoPtr>> TTableMountCache::GetTableInfo(const TYPath& path)
{
    return Impl_->GetTableInfo(path);
}

void TTableMountCache::Clear()
{
    Impl_->Clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

