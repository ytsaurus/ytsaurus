#include "stdafx.h"
#include "table_mount_cache.h"
#include "config.h"
#include "private.h"

#include <core/misc/string.h>
#include <core/misc/expiring_cache.h>

#include <core/concurrency/rw_spinlock.h>
#include <core/concurrency/delayed_executor.h>

#include <core/ytree/ypath.pb.h>

#include <ytlib/election/config.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/unversioned_row.h>

#ifdef YT_USE_LLVM
#include <ytlib/query_client/folding_profiler.h>
#include <ytlib/query_client/cg_types.h>
#include <ytlib/query_client/cg_fragment_compiler.h>
#include <ytlib/query_client/query_statistics.h>
#endif

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
using namespace NHive;
using namespace NNodeTrackerClient;
#ifdef YT_USE_LLVM
using namespace NQueryClient;
#endif

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
    : public TExpiringCache<TYPath, TTableMountInfoPtr>
{
public:
    TImpl(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel,
        TCellDirectoryPtr cellDirectory)
        : TExpiringCache(config)
        , Config_(config)
        , ObjectProxy_(masterChannel)
        , CellDirectory_(cellDirectory)
    { }

    TFuture<TTableMountInfoPtr> GetTableInfo(const TYPath& path)
    {
        return TExpiringCache::Get(path);
    }

    void Clear()
    {
        TExpiringCache::Clear();
        LOG_DEBUG("Table mount info cache cleared");
    }

private:
    const TTableMountCacheConfigPtr Config_;
    TObjectServiceProxy ObjectProxy_;
    const TCellDirectoryPtr CellDirectory_;


    virtual TFuture<TTableMountInfoPtr> DoGet(const TYPath& path) override
    {
        LOG_DEBUG("Requesting table mount info (Path: %v)",
            path);

        auto req = TTableYPathProxy::GetMountInfo(path);
        auto* cachingHeaderExt = req->Header().MutableExtension(TCachingHeaderExt::caching_header_ext);
        cachingHeaderExt->set_success_expiration_time(Config_->SuccessExpirationTime.MilliSeconds());
        cachingHeaderExt->set_failure_expiration_time(Config_->FailureExpirationTime.MilliSeconds());

        return ObjectProxy_.Execute(req).Apply(
            BIND([= , this_ = MakeStrong(this)] (const TTableYPathProxy::TErrorOrRspGetMountInfoPtr& rspOrError) {
                if (!rspOrError.IsOK()) {
                    auto wrappedError = TError("Error getting mount info for %v",
                        path)
                        << rspOrError;
                    LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                const auto& rsp = rspOrError.Value();
                auto tableInfo = New<TTableMountInfo>();
                tableInfo->Path = path;
                tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
                tableInfo->Schema = FromProto<TTableSchema>(rsp->schema());
                tableInfo->KeyColumns = FromProto<TKeyColumns>(rsp->key_columns());
                tableInfo->Sorted = rsp->sorted();
                tableInfo->NeedKeyEvaluation = tableInfo->Schema.HasComputedColumns();

                for (const auto& protoTabletInfo : rsp->tablets()) {
                    auto tabletInfo = New<TTabletInfo>();
                    tabletInfo->CellId = FromProto<TCellId>(protoTabletInfo.cell_id());
                    tabletInfo->TabletId = FromProto<TObjectId>(protoTabletInfo.tablet_id());
                    tabletInfo->State = ETabletState(protoTabletInfo.state());
                    tabletInfo->PivotKey = FromProto<TOwningKey>(protoTabletInfo.pivot_key());

                    if (protoTabletInfo.has_cell_id()) {
                        tabletInfo->CellId = FromProto<TCellId>(protoTabletInfo.cell_id());
                    }

                    tableInfo->Tablets.push_back(tabletInfo);
                }

                for (const auto& protoDescriptor : rsp->tablet_cells()) {
                    auto descriptor = FromProto<TCellDescriptor>(protoDescriptor);
                    if (CellDirectory_->ReconfigureCell(descriptor)) {
                        LOG_DEBUG("Hive cell reconfigured (CellId: %v, ConfigVersion: %v)",
                            descriptor.CellId,
                            descriptor.ConfigVersion);
                    }
                }

                LOG_DEBUG("Table mount info received (Path: %v, TableId: %v, TabletCount: %v, Sorted: %v)",
                    path,
                    tableInfo->TableId,
                    tableInfo->Tablets.size(),
                    tableInfo->Sorted);

                return tableInfo;
            }));
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

TFuture<TTableMountInfoPtr> TTableMountCache::GetTableInfo(const TYPath& path)
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

