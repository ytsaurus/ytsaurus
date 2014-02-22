#include "stdafx.h"
#include "table_mount_cache.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTabletClient {

using namespace NYPath;
using namespace NRpc;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NHive;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletClientLogger;

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

    TFuture<TErrorOr<TTableMountInfoPtr>> LookupTableInfo(const TYPath& path)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto it = PathToEntry.find(path);
        if (it == PathToEntry.end()) {
            TTableEntry entry;
            entry.Promise = NewPromise<TErrorOr<TTableMountInfoPtr>>();
            it = PathToEntry.insert(std::make_pair(path, entry)).first;
            RequestTableMountInfo(path);
            return entry.Promise;
        }

        auto& entry = it->second;
        if (!entry.Promise.IsSet()) {
            return entry.Promise;
        }

        const auto& infoOrError = entry.Promise.Get();
        auto now = TInstant::Now();
        if (infoOrError.IsOK()) {
            if (entry.Timestamp < now - Config_->SuccessExpirationTime) {
                // Return what we already have but refresh the cache in background.
                RequestTableMountInfo(path);
            }
        } else {
            if (entry.Timestamp < now - Config_->FailureExpirationTime) {
                // Evict and retry.
                UnrefTablets(entry);
                PathToEntry.erase(it);
                guard.Release();
                return LookupTableInfo(path);
            }
        }

        return entry.Promise;
    }


    TTabletInfoPtr FindTabletInfo(const TTabletId& id)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = TabletIdToEntry.find(id);
        return it == TabletIdToEntry.end() ? nullptr : it->second.TabletInfo;
    }

    TTabletInfoPtr GetTabletInfoOrThrow(const TTabletId& id)
    {
        auto info = FindTabletInfo(id);
        if (!info) {
            THROW_ERROR_EXCEPTION("Unknown tablet %s",
                ~ToString(id));
        }
        return info;
    }

private:
    TTableMountCacheConfigPtr Config_;
    TObjectServiceProxy ObjectProxy_;
    TCellDirectoryPtr CellDirectory_;
    

    struct TTableEntry
    {
        //! When this entry was last updated.
        TInstant Timestamp;
        //! Some latest known info (possibly not yet set).
        TPromise<TErrorOr<TTableMountInfoPtr>> Promise;
    };

    struct TTabletEntry
    {
        TTabletEntry()
            : UseCounter(0)
        { }

        //! Number of tables referring to this tablet id.
        int UseCounter; 
        //! Some latest known info.
        TTabletInfoPtr TabletInfo;
    };


    TSpinLock SpinLock_;
    yhash<TYPath, TTableEntry> PathToEntry;
    yhash<TTabletId, TTabletEntry> TabletIdToEntry;


    void RefTablet(TTabletInfoPtr tabletInfo)
    {
        auto it = TabletIdToEntry.find(tabletInfo->TabletId);
        if (it == TabletIdToEntry.end()) {
            TTabletEntry entry;
            it = TabletIdToEntry.insert(std::make_pair(tabletInfo->TabletId, entry)).first;
        }

        auto& entry = it->second;
        ++entry.UseCounter;
        entry.TabletInfo = std::move(tabletInfo);
    }

    void UnrefTablet(TTabletInfoPtr tabletInfo)
    {
        auto it = TabletIdToEntry.find(tabletInfo->TabletId);
        YCHECK(it != TabletIdToEntry.end());
        auto& entry = it->second;
        if (--entry.UseCounter == 0) {
            YCHECK(TabletIdToEntry.erase(tabletInfo->TabletId) == 1);
        }
    }

    void UnrefTablets(const TTableEntry& entry)
    {
        auto tableInfoOrError = entry.Promise.Get();
        if (!tableInfoOrError.IsOK())
            return;

        auto tableInfo = tableInfoOrError.Value();
        for (auto tabletInfo : tableInfo->Tablets) {
            UnrefTablet(std::move(tabletInfo));
        }
    }


    void RequestTableMountInfo(const TYPath& path)
    {
        LOG_DEBUG("Requesting table mount info for %s",
            ~path);

        auto req = TTableYPathProxy::GetMountInfo(path);
        ObjectProxy_.Execute(req).Subscribe(
            BIND(&TImpl::OnTableMountInfoResponse, MakeStrong(this), path));
    }

    void OnTableMountInfoResponse(const TYPath& path, TTableYPathProxy::TRspGetMountInfoPtr rsp)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = PathToEntry.find(path);
        if (it == PathToEntry.end())
            return;

        auto& entry = it->second;

        auto setResult = [&] (TErrorOr<TTableMountInfoPtr> result) {
            entry.Timestamp = TInstant::Now();
            if (entry.Promise.IsSet()) {
                UnrefTablets(entry);
                entry.Promise = MakePromise(result);
            } else {
                entry.Promise.Set(result);
            }
        };

        if (!rsp->IsOK()) {
            auto error = TError("Error getting mount info for %s",
                ~path)
                << *rsp;
            setResult(error);
            LOG_WARNING(error);
            return;
        }

        auto tableInfo = New<TTableMountInfo>();
        tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
        tableInfo->Schema = FromProto<TTableSchema>(rsp->schema());
        tableInfo->KeyColumns = FromProto<Stroka>(rsp->key_columns().names());
        tableInfo->Sorted = rsp->sorted();

        auto nodeDirectory = New<TNodeDirectory>();
        nodeDirectory->MergeFrom(rsp->node_directory());

        for (const auto& protoTabletInfo : rsp->tablets()) {
            auto tabletInfo = New<TTabletInfo>();
            tabletInfo->TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
            tabletInfo->State = ETabletState(protoTabletInfo.state());
            tabletInfo->PivotKey = FromProto<TOwningKey>(protoTabletInfo.pivot_key());
            
            if (protoTabletInfo.has_cell_id()) {
                tabletInfo->CellId = FromProto<TTabletCellId>(protoTabletInfo.cell_id()); 
            }
            
            if (protoTabletInfo.has_cell_config()) {
                CellDirectory_->RegisterCell(tabletInfo->CellId, protoTabletInfo.cell_config());
            }
            
            for (auto nodeId : protoTabletInfo.replica_node_ids()) {
                tabletInfo->Replicas.push_back(TTabletReplica(
                    nodeId,
                    nodeDirectory->GetDescriptor(nodeId)));
            }

            tableInfo->Tablets.push_back(tabletInfo);
            RefTablet(tabletInfo);
        }

        setResult(tableInfo);

        LOG_DEBUG("Table mount info received (Path: %s, TableId: %s, TabletCount: %d, Sorted: %s)",
            ~path,
            ~ToString(tableInfo->TableId),
            static_cast<int>(tableInfo->Tablets.size()),
            ~FormatBool(tableInfo->Sorted));
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

TFuture<TErrorOr<TTableMountInfoPtr>> TTableMountCache::LookupTableInfo(const TYPath& path)
{
    return Impl_->LookupTableInfo(path);
}

TTabletInfoPtr TTableMountCache::FindTabletInfo(const TTabletId& id)
{
    return Impl_->FindTabletInfo(id);
}

TTabletInfoPtr TTableMountCache::GetTabletInfoOrThrow(const TTabletId& id)
{
    return Impl_->GetTabletInfoOrThrow(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

