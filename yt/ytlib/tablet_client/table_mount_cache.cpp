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
using namespace NVersionedTableClient;
using namespace NHive;
using namespace NNodeTrackerClient;
using namespace NQueryClient;

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

void TTableMountInfo::EvaluateKeys(TUnversionedRow fullRow, TRowBuffer& buffer)
{
#ifdef YT_USE_LLVM
    for (int index = 0; index < KeyColumns.size(); ++index) {
        if (Schema.Columns()[index].Expression) {
            if (!Evaluators[index]) {
                //FIXME folding
                llvm::FoldingSetNodeID id;
                TCGBinding binding;
                auto expr = PrepareExpression(Schema.Columns()[index].Expression.Get(), Schema);
                TFoldingProfiler(id, binding, Variables[index]).Profile(expr);
                // FIXME check that all references are keys
                Evaluators[index] = CodegenExpression(expr, Schema, binding);
            }

            TQueryStatistics statistics;
            TExecutionContext executionContext;
            executionContext.Schema = Schema;
            executionContext.LiteralRows = &Variables[index].LiteralRows;
            executionContext.PermanentBuffer = &buffer;
            executionContext.OutputBuffer = &buffer;
            executionContext.IntermediateBuffer = &buffer;
            executionContext.Statistics = &statistics;

            Evaluators[index](&fullRow[index], fullRow, Variables[index].ConstantsRowBuilder.GetRow(), &executionContext);
        }
    }
#else
    THROW_ERROR_EXCEPTION("Computed colums require LLVM enabled in build");
#endif
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

    TFuture<TTableMountInfoPtr> GetTableInfo(const TYPath& path)
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
                auto promise = entry.Promise = NewPromise<TTableMountInfoPtr>();
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
        TPromise<TTableMountInfoPtr> Promise;
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

    void OnTableMountInfoResponse(const TYPath& path, const TTableYPathProxy::TErrorOrRspGetMountInfoPtr& rspOrError)
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

        if (!rspOrError.IsOK()) {
            auto wrappedError = TError("Error getting mount info for %v",
                path)
                << rspOrError;
            setResult(wrappedError);
            LOG_WARNING(wrappedError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto tableInfo = New<TTableMountInfo>();
        tableInfo->Path = path;
        tableInfo->TableId = FromProto<TObjectId>(rsp->table_id());
        tableInfo->Schema = FromProto<TTableSchema>(rsp->schema());
        tableInfo->KeyColumns = FromProto<TKeyColumns>(rsp->key_columns());
        tableInfo->Sorted = rsp->sorted();

        for (int index = 0; index < tableInfo->KeyColumns.size(); ++index) {
            if (tableInfo->Schema.Columns()[index].Expression) {
                tableInfo->NeedKeyEvaluation = true;
                break;
            }
        }
#ifdef YT_USE_LLVM
        if (tableInfo->NeedKeyEvaluation) {
            auto keyColumnCount = tableInfo->KeyColumns.size();
            tableInfo->Evaluators.resize(keyColumnCount);
            tableInfo->Variables.resize(keyColumnCount);
        }
#endif

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

        {
            NTracing::TNullTraceContextGuard guard;
            entry.ProbationCookie = TDelayedExecutor::Submit(
                BIND(&TImpl::RequestTableMountInfo, MakeWeak(this), path),
                Config_->SuccessProbationTime);
        }

        LOG_DEBUG("Table mount info received (Path: %v, TableId: %v, TabletCount: %v, Sorted: %v)",
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

