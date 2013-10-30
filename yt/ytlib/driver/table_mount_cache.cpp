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
        LOG_DEBUG("Requesting table mount info (Path: %s)",
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
            auto info = New<TTableMountInfo>();
            info->TableId = FromProto<TObjectId>(rsp->table_id());
            info->Schema = FromProto<TTableSchema>(rsp->schema());
            info->KeyColumns = FromProto<Stroka>(rsp->key_columns().names());

            if (rsp->has_tablet()) {
                const auto& tablet = rsp->tablet();
                info->TabletId = FromProto<TObjectId>(tablet.tablet_id());
                auto cellId = FromProto<TTabletCellId>(tablet.cell_id());
                CellDirectory->RegisterCell(cellId, tablet.cell_config());
            }

            LOG_DEBUG("Table mount info received (Path: %s, TableId: %s, TabletId: %s)",
                ~path,
                ~ToString(info->TableId),
                ~ToString(info->TabletId));
            entry.Promise.Set(TErrorOr<TTableMountInfoPtr>(info));
        } else {
            entry.Promise.Set(TError("Error getting mount info for %s",
                ~path)
                << *rsp);
            LOG_DEBUG(*rsp, "Error getting table mount info (Path: %s)",
                ~path);
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

TFuture<TErrorOr<TTableMountInfoPtr>> TTableMountCache::LookupInfo(const TYPath& path)
{
    return Impl->LookupInfo(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

