#include "stdafx.h"
#include "table_mount_cache.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/hive/cell_directory.h>

namespace NYT {
namespace NDriver {

using namespace NYPath;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NVersionedTableClient;
using namespace NHive;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel,
        TCellDirectoryPtr cellDirectory)
        : Config(config)
        , ObjectProxy(masterChannel)
        , CellDirectory(cellDirectory)
    { }

    TFuture<TErrorOr<TTableMountInfoPtr>> LookupInfo(const TYPath& path)
    {
        TGuard<TSpinLock> guard(Spinlock);

        auto it = TableMountInfoCache.find(path);
        if (it == TableMountInfoCache.end()) {
            TTableCacheEntry entry;
            entry.Promise = NewPromise<TErrorOr<TTableMountInfoPtr>>();
            it = TableMountInfoCache.insert(std::make_pair(path, entry)).first;
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
            if (entry.Timestamp < now - Config->SuccessExpirationTime) {
                // Return what we already have but refresh the cache in background.
                RequestTableMountInfo(path);
            }
        } else {
            if (entry.Timestamp < now - Config->FailureExpirationTime) {
                // Evict and retry.
                TableMountInfoCache.erase(it);
                guard.Release();
                return LookupInfo(path);
            }
        }

        return entry.Promise;
    }

private:
    TTableMountCacheConfigPtr Config;
    TObjectServiceProxy ObjectProxy;
    TCellDirectoryPtr CellDirectory;
    

    struct TTableCacheEntry
    {
        TInstant Timestamp;
        TPromise<TErrorOr<TTableMountInfoPtr>> Promise;
    };

    TSpinLock Spinlock;
    yhash<TYPath, TTableCacheEntry> TableMountInfoCache;

    void RequestTableMountInfo(const TYPath& path)
    {
        LOG_DEBUG("Requesting table mount info for %s",
            ~path);

        auto req = TTableYPathProxy::GetMountInfo(path);
        ObjectProxy.Execute(req).Subscribe(
            BIND(&TImpl::OnTableMountInfoResponse, MakeStrong(this), path));
    }

    void OnTableMountInfoResponse(const TYPath& path, TTableYPathProxy::TRspGetMountInfoPtr rsp)
    {
        TGuard<TSpinLock> guard(Spinlock);
        auto it = TableMountInfoCache.find(path);
        if (it == TableMountInfoCache.end())
            return;

        auto& entry = it->second;
        entry.Timestamp = TInstant::Now();
        if (rsp->IsOK()) {
            auto mountInfo = New<TTableMountInfo>();
            mountInfo->TableId = FromProto<TObjectId>(rsp->table_id());
            mountInfo->Schema = FromProto<TTableSchema>(rsp->schema());
            mountInfo->KeyColumns = FromProto<Stroka>(rsp->key_columns().names());

            for (const auto& protoTabletInfo : rsp->tablets()) {
                TTabletInfo tabletInfo;
                tabletInfo.TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
                tabletInfo.State = ETabletState(protoTabletInfo.state());
                tabletInfo.PivotKey = FromProto<TOwningKey>(protoTabletInfo.pivot_key());
                if (protoTabletInfo.has_cell_id()) {
                    tabletInfo.CellId = FromProto<TTabletCellId>(protoTabletInfo.cell_id()); 
                }
                if (protoTabletInfo.has_cell_config()) {
                    CellDirectory->RegisterCell(tabletInfo.CellId, protoTabletInfo.cell_config());
                }
                mountInfo->Tablets.push_back(tabletInfo);
            }

            TErrorOr<TTableMountInfoPtr> promiseResult(mountInfo);
            if (entry.Promise.IsSet()) {
                entry.Promise = MakePromise(promiseResult);
            } else {
                entry.Promise.Set(promiseResult);
            }

            LOG_DEBUG("Table mount info received (Path: %s, TableId: %s, TabletCount: %d)",
                ~path,
                ~ToString(mountInfo->TableId),
                static_cast<int>(mountInfo->Tablets.size()));
        } else {
            auto error = TError("Error getting mount info for %s",
                ~path)
                << *rsp;
            entry.Promise.Set(error);
            LOG_DEBUG(error);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TTableMountCache::TTableMountCache(
    TTableMountCacheConfigPtr config,
    IChannelPtr masterChannel,
    TCellDirectoryPtr cellDirectory)
    : Impl(New<TImpl>(
        config,
        masterChannel,
        cellDirectory))
{ }

TTableMountCache::~TTableMountCache()
{ }

TFuture<TErrorOr<TTableMountInfoPtr>> TTableMountCache::LookupInfo(const TYPath& path)
{
    return Impl->LookupInfo(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

