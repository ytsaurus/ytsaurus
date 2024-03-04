#include "client_impl.h"
#include "backup_session.h"
#include "config.h"
#include "connection.h"
#include "tablet_helpers.h"
#include "transaction.h"
#include "type_handler.h"

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/chaos_client/banned_replica_tracker.h>
#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_addresses_provider.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/query_client/executor.h>
#include <yt/yt/ytlib/query_client/explain.h>
#include <yt/yt/ytlib/query_client/functions_cache.h>
#include <yt/yt/ytlib/query_client/query_service_proxy.h>
#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/ytlib/tablet_client/pivot_keys_picker.h>
#include <yt/yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>
#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>
#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <library/cpp/int128/int128.h>

#include <util/random/random.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NQueueClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TString UpstreamReplicaIdAttributeName = "upstream_replica_id";
const TString SecondaryIndexAlias = "IndexTable";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryPreparer)

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TReq>
void SetDynamicTableCypressRequestFullPath(TReq* /*req*/, const TYPath& /*fullPath*/)
{ }

template <>
void SetDynamicTableCypressRequestFullPath<NTabletClient::NProto::TReqMount>(
    NTabletClient::NProto::TReqMount* req,
    const TYPath& fullPath)
{
    req->set_path(fullPath);
}

TColumnFilter RemapColumnFilter(
    const TColumnFilter& columnFilter,
    const TNameTableToSchemaIdMapping& idMapping,
    const TNameTablePtr& nameTable)
{
    if (columnFilter.IsUniversal()) {
        return columnFilter;
    }
    auto remappedFilterIndexes = columnFilter.GetIndexes();
    for (auto& index : remappedFilterIndexes) {
        if (index < 0 || index >= std::ssize(idMapping)) {
            THROW_ERROR_EXCEPTION(
                "Column filter contains invalid index: actual %v, expected in range [0, %v]",
                index,
                idMapping.size() - 1);
        }
        if (idMapping[index] == -1) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::ColumnNotFound,
                "Invalid column %Qv in column filter",
                nameTable->GetName(index));
        }
        index = idMapping[index];
    }
    return TColumnFilter(std::move(remappedFilterIndexes));
}

std::vector<TString> GetLookupColumns(const TColumnFilter& columnFilter, const TTableSchema& schema)
{
    std::vector<TString> columns;
    if (columnFilter.IsUniversal()) {
        columns.reserve(schema.Columns().size());
        for (const auto& columnSchema : schema.Columns()) {
            columns.push_back(columnSchema.Name());
        }
    } else {
        columns.reserve(columnFilter.GetIndexes().size());
        for (auto index : columnFilter.GetIndexes()) {
            columns.push_back(schema.Columns()[index].Name());
        }
    }
    return columns;
}

void RemapValueIds(
    TVersionedRow /*row*/,
    std::vector<TTypeErasedRow>& rows,
    const std::vector<int>& mapping)
{
    for (auto untypedRow : rows) {
        auto row = TMutableVersionedRow(untypedRow);
        if (!row) {
            continue;
        }
        for (auto& value : row.Keys()) {
            auto id = value.Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            value.Id = mapping[id];
        }
        for (auto& value : row.Values()) {
            auto id = value.Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            value.Id = mapping[id];
        }
    }
}

void RemapValueIds(
    TUnversionedRow /*row*/,
    std::vector<TTypeErasedRow>& rows,
    const std::vector<int>& mapping)
{
    for (auto untypedRow : rows) {
        auto row = TMutableUnversionedRow(untypedRow);
        if (!row) {
            continue;
        }
        for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
            auto id = row[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row[index].Id = mapping[id];
        }
    }
}

std::vector<int> BuildResponseIdMapping(const TColumnFilter& remappedColumnFilter)
{
    std::vector<int> mapping;
    for (int index = 0; index < std::ssize(remappedColumnFilter.GetIndexes()); ++index) {
        int id = remappedColumnFilter.GetIndexes()[index];
        if (id >= std::ssize(mapping)) {
            mapping.resize(id + 1, -1);
        }
        mapping[id] = index;
    }
    return mapping;
}

TTimestamp ExtractTimestampFromPulledRow(TVersionedRow row)
{
    auto writeTimestampCount = row.GetWriteTimestampCount();
    auto deleteTimestampCount = row.GetDeleteTimestampCount();

    if (writeTimestampCount > 1 || deleteTimestampCount > 1) {
        THROW_ERROR_EXCEPTION("Unexpected timestamps in pulled rows")
            << TErrorAttribute("write_timestamp_count", writeTimestampCount)
            << TErrorAttribute("delete_timestamp_count", deleteTimestampCount)
            << TErrorAttribute("key", ToOwningKey(row));
    }

    if (writeTimestampCount > 0 && deleteTimestampCount > 0) {
        auto writeTimestamp = row.WriteTimestamps()[0];
        auto deleteTimestamp = row.DeleteTimestamps()[0];
        if (writeTimestamp != deleteTimestamp) {
            THROW_ERROR_EXCEPTION("Timestamps mismatch in pulled row")
                << TErrorAttribute("write_timestamp", writeTimestamp)
                << TErrorAttribute("delete_timestamp", deleteTimestamp)
                << TErrorAttribute("key", ToOwningKey(row));
        }

        return writeTimestamp;
    }

    if (writeTimestampCount > 0) {
        return row.WriteTimestamps()[0];
    }
    if (deleteTimestampCount > 0) {
        return row.DeleteTimestamps()[0];
    }

    THROW_ERROR_EXCEPTION("Pulled a row without timestamps")
        << TErrorAttribute("key", ToOwningKey(row));
}


TSchemaUpdateEnabledFeatures GetSchemaUpdateEnabledFeatures()
{
    return TSchemaUpdateEnabledFeatures {
        true,  /*EnableStaticTableDropColumn*/
        true  /*EnableDynamicTableDropColumn*/
    };
}

class TStickyTableMountInfoCache final
{
public:
    explicit TStickyTableMountInfoCache(ITableMountCachePtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path)
    {
        auto guard = Guard(SpinLock_);

        return TableInfoMap_.contains(path)
            ? TableInfoMap_[path]
            : TableInfoMap_[path] = Underlying_->GetTableInfo(path);
    }

private:
    const ITableMountCachePtr Underlying_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<NYPath::TYPath, TFuture<TTableMountInfoPtr>> TableInfoMap_;
};

using TStickyTableMountInfoCachePtr = TIntrusivePtr<TStickyTableMountInfoCache>;

void TransformWithIndexStatement(NAst::TAstHead* head, TStickyTableMountInfoCachePtr cache)
{
    auto& query = std::get<NAst::TQuery>(head->Ast);
    if (!query.WithIndex) {
        return;
    }

    auto& index = *(query.WithIndex);

    auto indexTableInfo = WaitFor(cache->GetTableInfo(index.Path))
        .ValueOrThrow();
    auto tableInfo = WaitFor(cache->GetTableInfo(query.Table.Path))
        .ValueOrThrow();

    indexTableInfo->ValidateDynamic();
    indexTableInfo->ValidateSorted();

    const auto& indexTableSchema = *indexTableInfo->Schemas[ETableSchemaKind::Primary];
    const auto& tableSchema = *tableInfo->Schemas[ETableSchemaKind::Primary];

    const TColumnSchema* unfoldedColumn = nullptr;
    ValidateIndexSchema(tableSchema, indexTableSchema, &unfoldedColumn);

    index.Alias = SecondaryIndexAlias;
    const auto& alias = query.Table.Alias;

    if (unfoldedColumn) {
        NAst::TReference repeatedIndexedColumn(unfoldedColumn->Name(), alias);
        NAst::TReference unfoldedIndexerColumn(unfoldedColumn->Name(), index.Alias);

        query.WherePredicate = NAst::TListContainsTrasformer(
            head,
            repeatedIndexedColumn,
            unfoldedIndexerColumn)
            .Visit(query.WherePredicate);
    }

    NAst::TExpressionList indexJoinColumns;
    indexJoinColumns.reserve(tableSchema.GetKeyColumnCount());
    NAst::TExpressionList tableJoinColumns;
    tableJoinColumns.reserve(tableSchema.GetKeyColumnCount());
    THashSet<TString> replacedColumns;

    for (const auto& tableColumn : tableSchema.Columns()) {
        const auto* indexColumn = indexTableSchema.FindColumn(tableColumn.Name());

        if (indexColumn && *indexColumn->LogicalType() == *tableColumn.LogicalType()) {
            replacedColumns.insert(indexColumn->Name());
        }

        if (!tableColumn.SortOrder()) {
            continue;
        }

        YT_ASSERT(indexColumn && indexColumn->SortOrder());

        auto* indexReference = head->New<NAst::TReferenceExpression>(NullSourceLocation, indexColumn->Name(), index.Alias);
        auto* tableReference = head->New<NAst::TReferenceExpression>(NullSourceLocation, tableColumn.Name(), alias);

        indexJoinColumns.push_back(indexReference);
        tableJoinColumns.push_back(tableReference);
    }

    query.WherePredicate = NAst::TTableReferenceReplacer(
        head,
        std::move(replacedColumns),
        alias,
        index.Alias)
        .Visit(query.WherePredicate);

    std::swap(query.Table, index);
    query.Joins.insert(
        query.Joins.begin(),
        NAst::TJoin(
            /*isLeft*/ false,
            std::move(index),
            std::move(indexJoinColumns),
            std::move(tableJoinColumns),
            /*predicate*/ std::nullopt));

    query.WithIndex.reset();
}

std::vector<TTableMountInfoPtr> GetQueryTableInfos(
    NAst::TQuery* query,
    const TStickyTableMountInfoCachePtr& cache)
{
    std::vector<TYPath> paths{query->Table.Path};
    if (query->WithIndex) {
        paths.push_back(query->WithIndex->Path);
    }
    for (const auto& join : query->Joins) {
        if (const auto* tableJoin = std::get_if<NAst::TJoin>(&join)) {
            paths.push_back(tableJoin->Table.Path);
        }
    }

    std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
    for (const auto& path : paths) {
        asyncTableInfos.push_back(cache->GetTableInfo(path));
    }

    return WaitFor(AllSucceeded(asyncTableInfos))
        .ValueOrThrow();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TQueryPreparer
    : public virtual TRefCounted
    , public IPrepareCallbacks
{
public:
    TQueryPreparer(
        const TStickyTableMountInfoCachePtr& tableMountCache,
        IInvokerPtr invoker,
        TDetailedProfilingInfoPtr detailedProfilingInfo = nullptr,
        TSelectRowsOptions::TExpectedTableSchemas expectedTableSchemas = {})
        : TableMountCache_(std::move(tableMountCache))
        , Invoker_(std::move(invoker))
        , DetailedProfilingInfo_(std::move(detailedProfilingInfo))
        , ExpectedTableSchemas_(std::move(expectedTableSchemas))
    { }

    // IPrepareCallbacks implementation.
    TFuture<TDataSplit> GetInitialSplit(const TYPath& path) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(path);
    }

private:
    const TStickyTableMountInfoCachePtr& TableMountCache_;
    const IInvokerPtr Invoker_;
    const TDetailedProfilingInfoPtr DetailedProfilingInfo_;
    const TSelectRowsOptions::TExpectedTableSchemas ExpectedTableSchemas_;

    static TTableSchemaPtr GetTableSchema(
        const TRichYPath& path,
        const TTableMountInfoPtr& tableInfo)
    {
        if (auto pathSchema = path.GetSchema()) {
            if (tableInfo->Dynamic) {
                THROW_ERROR_EXCEPTION("Explicit YPath \"schema\" specification is only allowed for static tables");
            }
            return pathSchema;
        }

        return tableInfo->Schemas[ETableSchemaKind::Query];
    }

    TDataSplit DoGetInitialSplit(const TRichYPath& path)
    {
        NProfiling::TWallTimer timer;
        auto tableInfo = WaitFor(TableMountCache_->GetTableInfo(path.GetPath()))
            .ValueOrThrow();
        auto mountCacheWaitTime = timer.GetElapsedTime();

        if (DetailedProfilingInfo_ && tableInfo->EnableDetailedProfiling) {
            DetailedProfilingInfo_->EnableDetailedTableProfiling = true;
            DetailedProfilingInfo_->MountCacheWaitTime += mountCacheWaitTime;
        }

        tableInfo->ValidateNotPhysicallyLog();

        if (auto it = ExpectedTableSchemas_.find(path.GetPath())) {
            try {
                ValidateTableSchemaUpdateInternal(
                    *it->second,
                    *tableInfo->Schemas[ETableSchemaKind::Primary],
                    GetSchemaUpdateEnabledFeatures(),
                    true,
                    false);
            } catch (const std::exception& ex) {
                auto error = TError(NTabletClient::EErrorCode::TableSchemaIncompatible, "Schema validation failed during replica fallback")
                    << TErrorAttribute(UpstreamReplicaIdAttributeName, tableInfo->UpstreamReplicaId)
                    << ex;

                THROW_ERROR error;
            }
        }

        return TDataSplit{
            .ObjectId = tableInfo->TableId,
            .TableSchema = GetTableSchema(path, tableInfo),
            .MountRevision = tableInfo->PrimaryRevision,
        };
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryPreparer)

////////////////////////////////////////////////////////////////////////////////

std::vector<TTabletInfo> TClient::DoGetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    return GetTabletInfosImpl(tableInfo, tabletIndexes, options);
}

std::vector<TTabletInfo> TClient::GetTabletInfosByTabletIds(
    const TYPath& path,
    const std::vector<TTabletId>& tabletIds,
    const TGetTabletInfosOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    THashMap<TTabletId, int> tabletIdToTabletIndex;
    for (int tabletIndex = 0; tabletIndex < std::ssize(tableInfo->Tablets); ++tabletIndex) {
        tabletIdToTabletIndex[tableInfo->Tablets[tabletIndex]->TabletId] = tabletIndex;
    }

    std::vector<int> tabletIndexes;
    for (const auto& tabletId : tabletIds) {
        auto tabletIndex = tabletIdToTabletIndex.find(tabletId);

        // TODO(alexelex): retry due to reshard
        if (tabletIndex == tabletIdToTabletIndex.end()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                tabletId);
        }
        tabletIndexes.push_back(tabletIndex->second);
    }
    return GetTabletInfosImpl(tableInfo, tabletIndexes, options);
}

std::vector<TTabletInfo> TClient::GetTabletInfosImpl(
    const TTableMountInfoPtr& tableInfo,
    const std::vector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    tableInfo->ValidateDynamic();

    struct TTabletBatch
    {
        std::vector<TTabletId> TabletIds;
        std::vector<int> ResultIndexes;
    };

    THashMap<TCellId, TTabletBatch> cellIdToTabletBatch;
    std::vector<TCellId> cellIds;

    for (int resultIndex = 0; resultIndex < std::ssize(tabletIndexes); ++resultIndex) {
        auto tabletIndex = tabletIndexes[resultIndex];
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);

        auto [it, emplaced] = cellIdToTabletBatch.try_emplace(tabletInfo->CellId);
        if (emplaced) {
            cellIds.push_back(it->first);
        }
        it->second.TabletIds.push_back(tabletInfo->TabletId);
        it->second.ResultIndexes.push_back(resultIndex);
    }

    auto cellDescriptorsByPeer = GroupCellDescriptorsByPeer(Connection_, cellIds);

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> futures;
    futures.reserve(cellDescriptorsByPeer.size());
    for (const auto& cellDescriptors : cellDescriptorsByPeer) {
        auto channel = GetReadCellChannelOrThrow(cellDescriptors[0]);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetTabletInfosTimeout));

        auto req = proxy.GetTabletInfo();
        req->SetResponseHeavy(true);
        for (const auto& cellDescriptor : cellDescriptors) {
            auto cellId = cellDescriptor->CellId;
            const auto& tabletBatch = GetOrCrash(cellIdToTabletBatch, cellId);
            for (auto tabletId : tabletBatch.TabletIds) {
                ToProto(req->add_tablet_ids(), tabletId);
                ToProto(req->add_cell_ids(), cellId);
            }
        }

        req->set_request_errors(options.RequestErrors);
        futures.push_back(req->Invoke());
    }

    auto responses = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

    std::vector<TTabletInfo> results(tabletIndexes.size());
    for (int responseIndex = 0; responseIndex < std::ssize(responses); ++responseIndex) {
        const auto& response = responses[responseIndex];
        const auto& cellDescriptors = cellDescriptorsByPeer[responseIndex];

        int indexInResponse = 0;
        for (const auto& cellDescriptor : cellDescriptors) {
            auto cellId = cellDescriptor->CellId;
            const auto& tabletBatch = GetOrCrash(cellIdToTabletBatch, cellId);
            for (int resultIndex : tabletBatch.ResultIndexes) {
                auto& result = results[resultIndex];

                const auto& tabletInfo = response->tablets(indexInResponse++);

                result.TotalRowCount = tabletInfo.total_row_count();
                result.TrimmedRowCount = tabletInfo.trimmed_row_count();
                result.DelayedLocklessRowCount = tabletInfo.delayed_lockless_row_count();
                result.BarrierTimestamp = tabletInfo.barrier_timestamp();
                result.LastWriteTimestamp = tabletInfo.last_write_timestamp();
                result.TableReplicaInfos = tabletInfo.replicas().empty()
                    ? std::nullopt
                    : std::make_optional(std::vector<TTabletInfo::TTableReplicaInfo>());
                if (options.RequestErrors) {
                    FromProto(&result.TabletErrors, tabletInfo.tablet_errors());
                }

                for (const auto& protoReplicaInfo : tabletInfo.replicas()) {
                    auto& currentReplica = result.TableReplicaInfos->emplace_back();
                    currentReplica.ReplicaId = FromProto<TGuid>(protoReplicaInfo.replica_id());
                    currentReplica.LastReplicationTimestamp = protoReplicaInfo.last_replication_timestamp();
                    currentReplica.Mode = CheckedEnumCast<ETableReplicaMode>(protoReplicaInfo.mode());
                    currentReplica.CurrentReplicationRowIndex = protoReplicaInfo.current_replication_row_index();
                    currentReplica.CommittedReplicationRowIndex = protoReplicaInfo.committed_replication_row_index();
                    if (options.RequestErrors && protoReplicaInfo.has_replication_error()) {
                        FromProto(&currentReplica.ReplicationError, protoReplicaInfo.replication_error());
                    }
                }
            }
        }

        YT_VERIFY(indexInResponse == response->tablets_size());
    }

    return results;
}

TGetTabletErrorsResult TClient::DoGetTabletErrors(
    const TYPath& path,
    const TGetTabletErrorsOptions& options)
{
    auto masterReadOptions = TMasterReadOptions{
        .ReadFrom = EMasterChannelKind::Cache
    };

    auto proxy = CreateObjectServiceReadProxy(masterReadOptions);
    auto req = TTableYPathProxy::Get(path + "/@tablets");
    SetCachingHeader(req, masterReadOptions);

    auto tablets = WaitFor(proxy.Execute(req))
        .ValueOrThrow();
    auto tabletsNode = ConvertToNode(TYsonString(tablets->value()))->AsList();

    i64 errorCount = 0;
    i64 replicationErrorCount = 0;
    i64 limit = options.Limit.value_or(Connection_->GetConfig()->DefaultGetTabletErrorsLimit);
    bool incomplete = false;
    std::vector<TTabletId> tabletIdsToRequest;
    for (const auto& tablet : tabletsNode->GetChildren()) {
        auto tabletNode = tablet->AsMap();
        if (ConvertTo<ETabletState>(tabletNode->GetChildOrThrow("state")) == ETabletState::Unmounted) {
            continue;
        }

        auto tabletId = ConvertTo<TTabletId>(tabletNode->GetChildOrThrow("tablet_id"));

        if (errorCount <= limit &&
            tabletNode->GetChildOrThrow("error_count")->AsInt64()->GetValue() > 0)
        {
            if (errorCount < limit) {
                tabletIdsToRequest.push_back(tabletId);
            } else {
                incomplete = true;
            }
            ++errorCount;
        }

        if (replicationErrorCount <= limit &&
            tabletNode->GetChildOrThrow("replication_error_count")->AsInt64()->GetValue() > 0)
        {
            if (replicationErrorCount < limit) {
                if (tabletIdsToRequest.empty() || tabletIdsToRequest.back() != tabletId) {
                    tabletIdsToRequest.push_back(tabletId);
                }
            } else {
                incomplete = true;
            }
            ++replicationErrorCount;
        }

        if (replicationErrorCount >= limit && errorCount >= limit && incomplete) {
            break;
        }
    }

    auto tabletInfos = GetTabletInfosByTabletIds(
        path,
        tabletIdsToRequest,
        TGetTabletInfosOptions{{.Timeout = options.Timeout}, /*RequestErrors*/ true});

    TGetTabletErrorsResult result{.Incomplete = incomplete};
    for (int resultIndex = 0; resultIndex < std::ssize(tabletInfos); ++resultIndex) {
        if (!tabletInfos[resultIndex].TabletErrors.empty()) {
            result.TabletErrors[tabletIdsToRequest[resultIndex]] = std::move(tabletInfos[resultIndex].TabletErrors);
        }
        if (tabletInfos[resultIndex].TableReplicaInfos) {
            for (auto& replicaInfo : tabletInfos[resultIndex].TableReplicaInfos.value()) {
                if (!replicaInfo.ReplicationError.IsOK()) {
                    result.ReplicationErrors[replicaInfo.ReplicaId].push_back(std::move(replicaInfo.ReplicationError));
                }
            }
        }
    }

    return result;
}

TClient::TEncoderWithMapping TClient::GetLookupRowsEncoder() const
{
    return [] (
        const TColumnFilter& remappedColumnFilter,
        const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
    {
        NTableClient::NProto::TReqLookupRows req;
        if (remappedColumnFilter.IsUniversal()) {
            req.clear_column_filter();
        } else {
            ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteCommand(EWireProtocolCommand::LookupRows);
        writer->WriteMessage(req);
        writer->WriteSchemafulRowset(remappedKeys);
        return writer->Finish();
    };
}

TClient::TDecoderWithMapping TClient::GetLookupRowsDecoder() const
{
    return [] (
        const TSchemaData& schemaData,
        IWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadSchemafulRow(schemaData, true).ToTypeErasedRow();
    };
}

TUnversionedLookupRowsResult TClient::DoLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TLookupRowsOptions& options)
{
    TReplicaFallbackHandler<TUnversionedLookupRowsResult> fallbackHandler = [&] (
        const TReplicaFallbackInfo& replicaFallbackInfo)
    {
        auto unresolveOptions = options;
        unresolveOptions.ReplicaConsistency = EReplicaConsistency::None;
        unresolveOptions.FallbackTableSchema = replicaFallbackInfo.OriginalTableSchema;
        unresolveOptions.FallbackReplicaId = replicaFallbackInfo.ReplicaId;

        return replicaFallbackInfo.Client->LookupRows(
            replicaFallbackInfo.Path,
            nameTable,
            keys,
            unresolveOptions);
    };

    return CallAndRetryIfMetadataCacheIsInconsistent(
        options.DetailedProfilingInfo,
        [&] {
            return DoLookupRowsOnce<IUnversionedRowset, TUnversionedRow>(
                path,
                nameTable,
                keys,
                options,
                std::nullopt,
                GetLookupRowsEncoder(),
                GetLookupRowsDecoder(),
                fallbackHandler);
        });
}

TVersionedLookupRowsResult TClient::DoVersionedLookupRows(
    const TYPath& path,
    TNameTablePtr nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TVersionedLookupRowsOptions& options)
{
    TEncoderWithMapping encoder = [] (
        const TColumnFilter& remappedColumnFilter,
        const std::vector<TUnversionedRow>& remappedKeys) -> std::vector<TSharedRef>
    {
        NTableClient::NProto::TReqVersionedLookupRows req;
        if (remappedColumnFilter.IsUniversal()) {
            req.clear_column_filter();
        } else {
            ToProto(req.mutable_column_filter()->mutable_indexes(), remappedColumnFilter.GetIndexes());
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteCommand(EWireProtocolCommand::VersionedLookupRows);
        writer->WriteMessage(req);
        writer->WriteSchemafulRowset(remappedKeys);
        return writer->Finish();
    };

    TDecoderWithMapping decoder = [] (
        const TSchemaData& schemaData,
        IWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadVersionedRow(schemaData, true).ToTypeErasedRow();
    };

    TReplicaFallbackHandler<TVersionedLookupRowsResult> fallbackHandler = [&] (
        const TReplicaFallbackInfo& replicaFallbackInfo)
    {
        auto unresolveOptions = options;
        unresolveOptions.ReplicaConsistency = EReplicaConsistency::None;
        unresolveOptions.FallbackTableSchema = replicaFallbackInfo.OriginalTableSchema;
        unresolveOptions.FallbackReplicaId = replicaFallbackInfo.ReplicaId;

        return replicaFallbackInfo.Client->VersionedLookupRows(
            replicaFallbackInfo.Path,
            nameTable,
            keys,
            unresolveOptions);
    };

    std::optional<TString> retentionConfig;
    if (options.RetentionConfig) {
        retentionConfig = ConvertToYsonString(options.RetentionConfig).ToString();
    }

    if (options.RetentionTimestamp) {
        THROW_ERROR_EXCEPTION("Versioned lookup does not support retention timestamp");
    }

    return CallAndRetryIfMetadataCacheIsInconsistent(
        options.DetailedProfilingInfo,
        [&] {
            return DoLookupRowsOnce<IVersionedRowset, TVersionedRow>(
                path,
                nameTable,
                keys,
                options,
                retentionConfig,
                encoder,
                decoder,
                fallbackHandler);
        });
}

std::vector<TUnversionedLookupRowsResult> TClient::DoMultiLookupRows(
    const std::vector<TMultiLookupSubrequest>& subrequests,
    const TMultiLookupOptions& options)
{
    std::vector<TFuture<TUnversionedLookupRowsResult>> asyncResults;
    asyncResults.reserve(subrequests.size());
    for (const auto& subrequest : subrequests) {
        TLookupRowsOptions lookupRowsOptions;
        static_cast<TTabletReadOptionsBase&>(lookupRowsOptions) = options;
        static_cast<TMultiplexingBandOptions&>(lookupRowsOptions) = options;
        static_cast<TLookupRequestOptions&>(lookupRowsOptions) = std::move(subrequest.Options);

        asyncResults.push_back(BIND(
            [
                =,
                this,
                this_ = MakeStrong(this),
                lookupRowsOptions = std::move(lookupRowsOptions)
            ] {
                TReplicaFallbackHandler<TUnversionedLookupRowsResult> fallbackHandler = [&] (
                    const TReplicaFallbackInfo& replicaFallbackInfo)
                {
                    auto unresolveOptions = lookupRowsOptions;
                    unresolveOptions.ReplicaConsistency = EReplicaConsistency::None;
                    unresolveOptions.FallbackTableSchema = replicaFallbackInfo.OriginalTableSchema;
                    unresolveOptions.FallbackReplicaId = replicaFallbackInfo.ReplicaId;

                    return replicaFallbackInfo.Client->LookupRows(
                        replicaFallbackInfo.Path,
                        subrequest.NameTable,
                        subrequest.Keys,
                        unresolveOptions);
                };

                return CallAndRetryIfMetadataCacheIsInconsistent(
                    lookupRowsOptions.DetailedProfilingInfo,
                    [&] {
                        return DoLookupRowsOnce<IUnversionedRowset, TUnversionedRow>(
                            subrequest.Path,
                            subrequest.NameTable,
                            subrequest.Keys,
                            lookupRowsOptions,
                            /*retentionConfig*/ std::nullopt,
                            GetLookupRowsEncoder(),
                            GetLookupRowsDecoder(),
                            fallbackHandler);
                    });
            })
            .AsyncVia(GetCurrentInvoker())
            .Run());
    }

    return WaitFor(AllSucceeded(std::move(asyncResults)))
        .ValueOrThrow();
}

template <class IRowset, class TRow>
TLookupRowsResult<IRowset> TClient::DoLookupRowsOnce(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TLookupRowsOptionsBase& options,
    const std::optional<TString>& retentionConfig,
    TEncoderWithMapping encoderWithMapping,
    TDecoderWithMapping decoderWithMapping,
    TReplicaFallbackHandler<TLookupRowsResult<IRowset>> replicaFallbackHandler)
{
    if (options.EnablePartialResult && options.KeepMissingRows) {
        THROW_ERROR_EXCEPTION("Options \"enable_partial_result\" and \"keep_missing_rows\" cannot be used together");
    }

    if (options.RetentionTimestamp > options.Timestamp) {
        THROW_ERROR_EXCEPTION("Retention timestamp cannot be greater than read timestamp")
            << TErrorAttribute("retention_timestamp", options.RetentionTimestamp)
            << TErrorAttribute("timestamp", options.Timestamp);
    }

    const auto& connectionConfig = Connection_->GetConfig();

    const auto& tableMountCache = Connection_->GetTableMountCache();
    NProfiling::TWallTimer timer;
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();
    auto mountCacheWaitTime = timer.GetElapsedTime();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];

    if (options.FallbackReplicaId && tableInfo->UpstreamReplicaId != options.FallbackReplicaId) {
        THROW_ERROR_EXCEPTION("Invalid upstream replica id for chosen sync replica %Qv: expected %v, got %v",
            path,
            options.FallbackReplicaId,
            tableInfo->UpstreamReplicaId);
    }

    if (options.FallbackTableSchema && tableInfo->ReplicationCardId) {
        ValidateTableSchemaUpdateInternal(
            *options.FallbackTableSchema,
            *tableInfo->Schemas[ETableSchemaKind::Primary],
            GetSchemaUpdateEnabledFeatures(),
            true,
            false);
    }

    auto idMapping = BuildColumnIdMapping(*schema, nameTable);

    auto remappedColumnFilter = RemapColumnFilter(options.ColumnFilter, idMapping, nameTable);
    auto resultSchema = tableInfo->Schemas[ETableSchemaKind::Primary]->Filter(remappedColumnFilter, true);
    auto resultSchemaData = IWireProtocolReader::GetSchemaData(*schema, remappedColumnFilter);

    timer.Restart();
    NSecurityClient::TPermissionKey permissionKey{
        .Object = FromObjectId(tableInfo->TableId),
        .User = Options_.GetAuthenticatedUser(),
        .Permission = EPermission::Read,
        .Columns = GetLookupColumns(remappedColumnFilter, *schema)
    };
    const auto& permissionCache = Connection_->GetPermissionCache();
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError();
    auto permissionCacheWaitTime = timer.GetElapsedTime();

    if (options.DetailedProfilingInfo && tableInfo->EnableDetailedProfiling) {
        options.DetailedProfilingInfo->EnableDetailedTableProfiling = true;
        options.DetailedProfilingInfo->TablePath = path;
        options.DetailedProfilingInfo->MountCacheWaitTime = mountCacheWaitTime;
        options.DetailedProfilingInfo->PermissionCacheWaitTime = permissionCacheWaitTime;
    }

    if (keys.Empty()) {
        return {
            .Rowset = CreateRowset(resultSchema, TSharedRange<TRow>()),
        };
    }

    // NB: The server-side requires the keys to be sorted.
    using TSortedKey = std::pair<NTableClient::TLegacyKey, int>;
    std::vector<TSortedKey> sortedKeys;
    sortedKeys.reserve(keys.Size());
    auto sortedKeysGuard = TMemoryUsageTrackerGuard::TryAcquire(
        LookupMemoryTracker_,
        keys.Size() * sizeof(TSortedKey))
        .ValueOrThrow();

    struct TLookupRowsInputBufferTag
    { };
    auto inputRowBuffer = New<TRowBuffer>(
        TLookupRowsInputBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        LookupMemoryTracker_);

    auto evaluatorCache = Connection_->GetColumnEvaluatorCache();
    auto evaluator = tableInfo->NeedKeyEvaluation ? evaluatorCache->Find(schema) : nullptr;

    for (int index = 0; index < std::ssize(keys); ++index) {
        ValidateClientKey(keys[index], *schema, idMapping, nameTable);
        auto capturedKey = inputRowBuffer->CaptureAndPermuteRow(
            keys[index],
            *schema,
            schema->GetKeyColumnCount(),
            idMapping,
            nullptr);

        if (evaluator) {
            evaluator->EvaluateKeys(capturedKey, inputRowBuffer);
        }

        sortedKeys.emplace_back(capturedKey, index);
    }

    if (tableInfo->IsReplicated() ||
        tableInfo->IsChaosReplicated() ||
        (tableInfo->ReplicationCardId && options.ReplicaConsistency == EReplicaConsistency::Sync))
    {
        auto bannedReplicaTracker = static_cast<bool>(tableInfo->ReplicationCardId) || tableInfo->IsChaosReplicated()
            ? Connection_->GetBannedReplicaTrackerCache()->GetTracker(tableInfo->TableId)
            : nullptr;

        auto pickInSyncReplicas = [&] {
            if (tableInfo->ReplicationCardId) {
                auto replicationCard = GetSyncReplicationCard(tableInfo);
                bannedReplicaTracker->SyncReplicas(replicationCard);

                auto replicaIds = GetChaosTableInSyncReplicas(
                    tableInfo,
                    replicationCard,
                    nameTable,
                    MakeSharedRange(keys),
                    /*allKeys*/ false,
                    options.Timestamp);

                YT_LOG_DEBUG("Picked in-sync replicas for lookup (ReplicaIds: %v, Timestamp: %v, ReplicationCard: %v)",
                    replicaIds,
                    options.Timestamp,
                    *replicationCard);

                TTableReplicaInfoPtrList inSyncReplicas;
                for (auto replicaId : replicaIds) {
                    const auto& replica = GetOrCrash(replicationCard->Replicas, replicaId);
                    auto replicaInfo = New<TTableReplicaInfo>();
                    replicaInfo->ReplicaId = replicaId;
                    replicaInfo->ClusterName = replica.ClusterName;
                    replicaInfo->ReplicaPath = replica.ReplicaPath;
                    replicaInfo->Mode = replica.Mode;
                    inSyncReplicas.push_back(std::move(replicaInfo));
                }
                return inSyncReplicas;
            } else {
                auto inSyncReplicasFuture = PickInSyncReplicas(
                    Connection_,
                    tableInfo,
                    options,
                    sortedKeys);

                return WaitForFast(inSyncReplicasFuture)
                    .ValueOrThrow();
            }
        };

        auto pickedSyncReplicas = pickInSyncReplicas();
        TErrorOr<TLookupRowsResult<IRowset>> resultOrError;

        auto retryCountLimit = tableInfo->ReplicationCardId
            ? connectionConfig->ReplicaFallbackRetryCount
            : 0;

        for (int retryCount = 0; retryCount <= retryCountLimit; ++retryCount) {
            TTableReplicaInfoPtrList inSyncReplicas;
            std::vector<TTableReplicaId> bannedSyncReplicaIds;
            for (const auto& replicaInfo : pickedSyncReplicas) {
                if (bannedReplicaTracker && bannedReplicaTracker->IsReplicaBanned(replicaInfo->ReplicaId)) {
                    bannedSyncReplicaIds.push_back(replicaInfo->ReplicaId);
                } else {
                    inSyncReplicas.push_back(replicaInfo);
                }
            }

            if (inSyncReplicas.empty()) {
                std::vector<TError> replicaErrors;
                for (auto bannedReplicaId : bannedSyncReplicaIds) {
                    if (auto error = bannedReplicaTracker->GetReplicaError(bannedReplicaId); !error.IsOK()) {
                        replicaErrors.push_back(std::move(error));
                    }
                }

                auto error = TError(
                    NTabletClient::EErrorCode::NoInSyncReplicas,
                    "No in-sync replicas found for table %v",
                    tableInfo->Path)
                    << TErrorAttribute("banned_replicas", bannedSyncReplicaIds);
                *error.MutableInnerErrors() = std::move(replicaErrors);
                THROW_ERROR error;
            }

            auto replicaFallbackInfo = GetReplicaFallbackInfo(inSyncReplicas);
            replicaFallbackInfo.OriginalTableSchema = schema;

            resultOrError = WaitFor(replicaFallbackHandler(replicaFallbackInfo));
            if (resultOrError.IsOK()) {
                return resultOrError.Value();
            }

            YT_LOG_DEBUG(resultOrError, "Fallback to replica failed (ReplicaId: %v)",
                replicaFallbackInfo.ReplicaId);

            if (bannedReplicaTracker) {
                bannedReplicaTracker->BanReplica(replicaFallbackInfo.ReplicaId, resultOrError.Truncate());
            }
        }

        YT_VERIFY(!resultOrError.IsOK());
        resultOrError.ThrowOnError();
    } else if (tableInfo->IsReplicationLog()) {
        THROW_ERROR_EXCEPTION("Lookup from queue replica is not supported");
    }

    // TODO(sandello): Use code-generated comparer here.
    std::sort(sortedKeys.begin(), sortedKeys.end());
    std::vector<size_t> keyIndexToResultIndex(keys.Size());
    size_t currentResultIndex = 0;

    struct TLookupRowsOutputBufferTag
    { };
    auto outputRowBuffer = New<TRowBuffer>(
        TLookupRowsOutputBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        LookupMemoryTracker_);

    std::vector<TTypeErasedRow> uniqueResultRows;

    struct TBatch
    {
        NObjectClient::TObjectId TabletId;
        NHydra::TRevision MountRevision = NHydra::NullRevision;
        std::vector<TLegacyKey> Keys;
        size_t OffsetInResult;

        TQueryServiceProxy::TRspMultireadPtr Response;
    };

    std::vector<std::vector<TBatch>> batchesByCells;
    THashMap<TCellId, size_t> cellIdToBatchIndex;
    std::vector<TCellId> cellIds;

    auto inMemoryMode = EInMemoryMode::None;

    {
        auto itemsBegin = sortedKeys.begin();
        auto itemsEnd = sortedKeys.end();

        size_t keySize = schema->GetKeyColumnCount();

        itemsBegin = std::lower_bound(
            itemsBegin,
            itemsEnd,
            tableInfo->LowerCapBound.Get(),
            [&] (const auto& item, TLegacyKey pivot) {
                return CompareRows(item.first, pivot, keySize) < 0;
            });

        itemsEnd = std::upper_bound(
            itemsBegin,
            itemsEnd,
            tableInfo->UpperCapBound.Get(),
            [&] (TLegacyKey pivot, const auto& item) {
                return CompareRows(pivot, item.first, keySize) < 0;
            });

        auto nextShardIt = tableInfo->Tablets.begin() + 1;
        for (auto itemsIt = itemsBegin; itemsIt != itemsEnd;) {
            YT_VERIFY(!tableInfo->Tablets.empty());

            // Run binary search to find the relevant tablets.
            nextShardIt = std::upper_bound(
                nextShardIt,
                tableInfo->Tablets.end(),
                itemsIt->first,
                [&] (TLegacyKey key, const TTabletInfoPtr& tabletInfo) {
                    return CompareRows(key, tabletInfo->PivotKey.Get(), keySize) < 0;
                });

            const auto& startShard = *(nextShardIt - 1);
            auto nextPivotKey = (nextShardIt == tableInfo->Tablets.end())
                ? tableInfo->UpperCapBound
                : (*nextShardIt)->PivotKey;

            // Binary search to reduce expensive row comparisons
            auto endItemsIt = std::lower_bound(
                itemsIt,
                itemsEnd,
                nextPivotKey.Get(),
                [&] (const auto& item, TLegacyKey pivot) {
                    return CompareRows(item.first, pivot) < 0;
                });

            ValidateTabletMountedOrFrozen(tableInfo, startShard);

            auto [it, emplaced] = cellIdToBatchIndex.emplace(startShard->CellId, batchesByCells.size());
            if (emplaced) {
                batchesByCells.emplace_back();
                cellIds.push_back(it->first);
            }

            TBatch batch;
            batch.TabletId = startShard->TabletId;
            batch.MountRevision = startShard->MountRevision;
            batch.OffsetInResult = currentResultIndex;

            // Take an arbitrary one; these are all the same.
            inMemoryMode = startShard->InMemoryMode;

            std::vector<TLegacyKey> rows;
            rows.reserve(endItemsIt - itemsIt);
            auto rowsGuard = TMemoryUsageTrackerGuard::TryAcquire(
                LookupMemoryTracker_,
                rows.capacity() * sizeof(TLegacyKey))
                .ValueOrThrow();

            while (itemsIt != endItemsIt) {
                auto key = itemsIt->first;
                rows.push_back(key);

                do {
                    keyIndexToResultIndex[itemsIt->second] = currentResultIndex;
                    ++itemsIt;
                } while (itemsIt != endItemsIt && itemsIt->first == key);
                ++currentResultIndex;
            }

            batch.Keys = std::move(rows);
            batchesByCells[it->second].push_back(std::move(batch));
        }
    }

    using TEncoder = std::function<std::vector<TSharedRef>(const std::vector<NTableClient::TUnversionedRow>&)>;
    using TDecoder = std::function<NTableClient::TTypeErasedRow(NTableClient::IWireProtocolReader*)>;

    TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
    TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);

    auto* codec = NCompression::GetCodec(connectionConfig->LookupRowsRequestCodec);

    auto cellDescriptorsByPeer = GroupCellDescriptorsByPeer(Connection_, cellIds);

    const auto& networks = Connection_->GetNetworks();

    std::vector<TFuture<TQueryServiceProxy::TRspMultireadPtr>> multireadFutures;
    multireadFutures.reserve(cellDescriptorsByPeer.size());

    for (const auto& cellDescriptors : cellDescriptorsByPeer) {
        auto channel = CreateTabletReadChannel(
            ChannelFactory_,
            *cellDescriptors[0],
            options,
            networks);

        auto timeout = options.Timeout.value_or(connectionConfig->DefaultLookupRowsTimeout);
        if (options.EnablePartialResult) {
            timeout -= connectionConfig->LookupRowsRequestTimeoutSlack;
        }
        if (timeout == TDuration::Zero()) {
            TError error(NYT::EErrorCode::Timeout, "Multiread request timed out before being run");
            multireadFutures.push_back(MakeFuture<TQueryServiceProxy::TRspMultireadPtr>(error));
            continue;
        }

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(timeout);
        proxy.SetDefaultAcknowledgementTimeout(std::nullopt);

        auto req = proxy.Multiread();
        req->SetMultiplexingBand(options.MultiplexingBand);
        req->set_request_codec(ToProto<int>(connectionConfig->LookupRowsRequestCodec));
        req->set_response_codec(ToProto<int>(connectionConfig->LookupRowsResponseCodec));
        req->set_timestamp(options.Timestamp);
        req->set_retention_timestamp(options.RetentionTimestamp);
        req->set_enable_partial_result(options.EnablePartialResult);
        if (options.UseLookupCache) {
            req->set_use_lookup_cache(*options.UseLookupCache);
        }

        if (inMemoryMode == EInMemoryMode::None) {
            if (auto timeout = connectionConfig->LookupRowsExtMemoryLoggingSuppressionTimeout) {
                req->Header().set_logging_suppression_timeout(ToProto<i64>(*timeout));
            }
        } else {
            req->Header().set_uncancelable(true);
            if (auto timeout = connectionConfig->LookupRowsInMemoryLoggingSuppressionTimeout) {
                req->Header().set_logging_suppression_timeout(ToProto<i64>(*timeout));
            }
        }

        if (retentionConfig) {
            req->set_retention_config(*retentionConfig);
        }

        for (const auto& cellDescriptor : cellDescriptors) {
            auto cellId = cellDescriptor->CellId;
            int batchIndex = cellIdToBatchIndex[cellId];
            for (const auto& batch : batchesByCells[batchIndex]) {
                ToProto(req->add_cell_ids(), cellId);
                ToProto(req->add_tablet_ids(), batch.TabletId);
                req->add_mount_revisions(batch.MountRevision);
                auto requestData = codec->Compress(boundEncoder(batch.Keys));
                req->Attachments().push_back(requestData);
            }
        }

        auto* ext = req->Header().MutableExtension(NQueryClient::NProto::TReqMultireadExt::req_multiread_ext);
        ext->set_in_memory_mode(ToProto<int>(inMemoryMode));

        multireadFutures.push_back(req->Invoke());
    }

    auto results = WaitFor(AllSet(std::move(multireadFutures)))
        .ValueOrThrow();

    if (!options.EnablePartialResult && options.DetailedProfilingInfo) {
        int failedSubrequestCount = 0;
        for (int channelIndex = 0; channelIndex < std::ssize(results); ++channelIndex) {
            if (!results[channelIndex].IsOK()) {
                ++failedSubrequestCount;
            }
        }
        if (failedSubrequestCount > 0) {
            options.DetailedProfilingInfo->WastedSubrequestCount += std::ssize(results) - failedSubrequestCount;
        }
    }

    uniqueResultRows.resize(currentResultIndex, TTypeErasedRow{nullptr});

    auto* responseCodec = NCompression::GetCodec(connectionConfig->LookupRowsResponseCodec);

    for (int channelIndex = 0; channelIndex < ssize(results); ++channelIndex) {
        if (options.EnablePartialResult && !results[channelIndex].IsOK()) {
            continue;
        }

        const auto& result = results[channelIndex].ValueOrThrow();

        int batchOffset = 0;
        for (const auto& cellDescriptor : cellDescriptorsByPeer[channelIndex]) {
            auto cellId = cellDescriptor->CellId;
            const auto& batches = batchesByCells[cellIdToBatchIndex[cellId]];
            for (int localBatchIndex = 0; localBatchIndex < ssize(batches); ++localBatchIndex) {
                const auto& attachment = result->Attachments()[batchOffset + localBatchIndex];

                if (options.EnablePartialResult && attachment.Empty()) {
                    continue;
                }

                auto responseData = responseCodec->Decompress(attachment);
                auto responseDataGuard = TMemoryUsageTrackerGuard::TryAcquire(
                    LookupMemoryTracker_,
                    responseData.Size())
                    .ValueOrThrow();

                auto reader = CreateWireProtocolReader(responseData, outputRowBuffer);

                const auto& batch = batches[localBatchIndex];

                for (size_t index = 0; index < batch.Keys.size(); ++index) {
                    uniqueResultRows[batch.OffsetInResult + index] = boundDecoder(reader.get());
                }
            }
            batchOffset += ssize(batches);
        }
    }

    if (!remappedColumnFilter.IsUniversal()) {
        RemapValueIds(TRow(), uniqueResultRows, BuildResponseIdMapping(remappedColumnFilter));
    }

    std::vector<TTypeErasedRow> resultRows;
    resultRows.resize(keys.Size());

    for (int index = 0; index < std::ssize(keys); ++index) {
        resultRows[index] = uniqueResultRows[keyIndexToResultIndex[index]];
    }

    if (!options.KeepMissingRows && !options.EnablePartialResult) {
        resultRows.erase(
            std::remove_if(
                resultRows.begin(),
                resultRows.end(),
                [] (TTypeErasedRow row) {
                    return !static_cast<bool>(row);
                }),
            resultRows.end());
    }

    auto rowRange = ReinterpretCastRange<TRow>(
        MakeSharedRange(std::move(resultRows), std::move(outputRowBuffer)));
    return {
        .Rowset = CreateRowset(resultSchema, std::move(rowRange)),
    };
}

TSelectRowsResult TClient::DoSelectRows(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    return CallAndRetryIfMetadataCacheIsInconsistent(
        options.DetailedProfilingInfo,
        [&] {
            return DoSelectRowsOnce(queryString, options);
        });
}

TSelectRowsResult TClient::DoSelectRowsOnce(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    if (options.RetentionTimestamp > options.Timestamp) {
        THROW_ERROR_EXCEPTION("Retention timestamp cannot be greater than read timestamp")
            << TErrorAttribute("retention_timestamp", options.RetentionTimestamp)
            << TErrorAttribute("timestamp", options.Timestamp);
    }

    auto parsedQuery = ParseSource(
        queryString,
        EParseMode::Query,
        options.PlaceholderValues,
        options.SyntaxVersion);
    auto* astQuery = &std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);

    auto cache = New<TStickyTableMountInfoCache>(Connection_->GetTableMountCache());
    GetQueryTableInfos(astQuery, cache);

    TransformWithIndexStatement(&parsedQuery->AstHead, cache);

    auto [tableInfos, replicaCandidates] = PrepareInSyncReplicaCandidates(
        options,
        GetQueryTableInfos(astQuery, cache));
    if (!tableInfos.empty()) {
        std::vector<TYPath> paths;
        for (const auto& tableInfo : tableInfos) {
            paths.push_back(tableInfo->Path);
        }

        auto unresolveOptions = options;
        unresolveOptions.ReplicaConsistency = EReplicaConsistency::None;

        int retryCountLimit = 0;
        for (const auto& tableInfo : tableInfos) {
            if (tableInfo->ReplicationCardId) {
                retryCountLimit = Connection_->GetConfig()->ReplicaFallbackRetryCount;
                break;
            }
        }

        THashMap<TTableReplicaId, TTableId> replicaIdToTableId;
        for (int index = 0; index < std::ssize(tableInfos); ++index) {
            const auto& tableInfo = tableInfos[index];
            if (tableInfo->ReplicationCardId) {
                for (const auto& replicaInfo : replicaCandidates[index]) {
                    replicaIdToTableId[replicaInfo->ReplicaId] = tableInfo->TableId;
                }
            }
        }

        TErrorOr<TSelectRowsResult> resultOrError;
        for (int retryCount = 0; retryCount <= retryCountLimit; ++retryCount) {
            YT_LOG_DEBUG("Picking cluster for replica fallback (Tables: %v, Attempt: %v)",
                paths,
                retryCount);

            auto [clusterName, expectedTableSchemas] = PickInSyncClusterAndPatchQuery(
                tableInfos,
                replicaCandidates,
                astQuery);
            unresolveOptions.ExpectedTableSchemas = std::move(expectedTableSchemas);

            auto replicaClient = GetOrCreateReplicaClient(clusterName);
            auto updatedQueryString = NAst::FormatQuery(*astQuery);
            auto asyncResult = replicaClient->SelectRows(updatedQueryString, unresolveOptions);
            resultOrError = WaitFor(asyncResult);
            if (resultOrError.IsOK()) {
                return resultOrError.Value();
            }

            if (auto schemaError = resultOrError.FindMatching(NTabletClient::EErrorCode::TableSchemaIncompatible)) {
                if (auto replicaId = schemaError->Attributes().Find<TTableReplicaId>(UpstreamReplicaIdAttributeName)) {
                    if (auto it = replicaIdToTableId.find(*replicaId)) {
                        auto bannedReplicaTracker = Connection_->GetBannedReplicaTrackerCache()->GetTracker(it->second);
                        bannedReplicaTracker->BanReplica(*replicaId, resultOrError.Truncate());
                    }
                }
            }
        }

        YT_VERIFY(!resultOrError.IsOK());
        resultOrError.ThrowOnError();
    }

    auto inputRowLimit = options.InputRowLimit.value_or(Connection_->GetConfig()->DefaultInputRowLimit);
    auto outputRowLimit = options.OutputRowLimit.value_or(Connection_->GetConfig()->DefaultOutputRowLimit);

    auto udfRegistryPath = options.UdfRegistryPath
        ? *options.UdfRegistryPath
        : Connection_->GetConfig()->UdfRegistryPath;

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());

        std::vector<TString> externalNames;
        for (const auto& name : names) {
            auto found = typeInferrers->find(name);
            if (found == typeInferrers->end()) {
                externalNames.push_back(name);
            }
        }

        auto descriptors = WaitFor(FunctionRegistry_->FetchFunctions(udfRegistryPath, externalNames))
            .ValueOrThrow();

        AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
    };

    auto queryPreparer = New<TQueryPreparer>(
        cache,
        Connection_->GetInvoker(),
        options.DetailedProfilingInfo,
        options.ExpectedTableSchemas);

    auto readSessionId = TReadSessionId::Create();

    auto memoryChunkProvider = MemoryProvider_->GetProvider(
            ToString(readSessionId),
            options.MemoryLimitPerNode,
            QueryMemoryTracker_);

    auto queryExecutor = CreateQueryExecutor(
        memoryChunkProvider,
        Connection_,
        Connection_->GetInvoker(),
        Connection_->GetColumnEvaluatorCache(),
        Connection_->GetQueryEvaluator(),
        ChannelFactory_,
        FunctionImplCache_.Get());

    auto fragment = PreparePlanFragment(
        queryPreparer.Get(),
        *parsedQuery,
        fetchFunctions,
        QueryMemoryTracker_);
    const auto& query = fragment->Query;
    const auto& dataSource = fragment->DataSource;

    THROW_ERROR_EXCEPTION_IF(
        query->GetTableSchema()->HasComputedColumns() && options.UseCanonicalNullRelations,
        "Currently queries with canonical null relations aren't allowed on tables with computed columns");

    for (size_t index = 0; index < query->JoinClauses.size(); ++index) {
        if (!query->JoinClauses[index]->ArrayExpressions.empty()) {
            continue;
        }
        if (query->JoinClauses[index]->ForeignKeyPrefix == 0 && !options.AllowJoinWithoutIndex) {
            const auto& ast = std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);
            THROW_ERROR_EXCEPTION("Foreign table key is not used in the join clause; "
                "the query is inefficient, consider rewriting it")
                << TErrorAttribute("source", NAst::FormatJoin(std::get<NAst::TJoin>(ast.Joins[index])));
        }
    }

    std::vector<NSecurityClient::TPermissionKey> permissionKeys;

    auto addTableForPermissionCheck = [&] (TTableId id, const TMappedSchema& schema) {
        std::vector<TString> columns;
        columns.reserve(schema.Mapping.size());
        for (const auto& columnDescriptor : schema.Mapping) {
            columns.push_back(schema.Original->Columns()[columnDescriptor.Index].Name());
        }
        permissionKeys.push_back(NSecurityClient::TPermissionKey{
            .Object = FromObjectId(id),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Read,
            .Columns = std::move(columns)
        });
    };
    addTableForPermissionCheck(dataSource.ObjectId, query->Schema);
    for (const auto& joinClause : query->JoinClauses) {
        if (joinClause->ArrayExpressions.empty()) {
            addTableForPermissionCheck(joinClause->ForeignObjectId, joinClause->Schema);
        }
    }

    if (options.ExecutionPool) {
        permissionKeys.push_back(NSecurityClient::TPermissionKey{
            .Object = QueryPoolsPath + "/" + NYPath::ToYPathLiteral(*options.ExecutionPool),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Use
        });
    }

    NProfiling::TWallTimer timer;
    const auto& permissionCache = Connection_->GetPermissionCache();
    auto permissionCheckErrors = WaitFor(permissionCache->GetMany(permissionKeys))
        .ValueOrThrow();
    for (const auto& error : permissionCheckErrors) {
        if (error.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }
        error.ThrowOnError();
    }
    auto permissionCacheWaitTime = timer.GetElapsedTime();

    if (options.DetailedProfilingInfo) {
        const auto& path = astQuery->Table.Path;
        timer.Restart();
        auto tableInfo = WaitFor(cache->GetTableInfo(path))
            .ValueOrThrow();
        auto mountCacheWaitTime = timer.GetElapsedTime();
        if (tableInfo->EnableDetailedProfiling) {
            options.DetailedProfilingInfo->EnableDetailedTableProfiling = true;
            options.DetailedProfilingInfo->TablePath = path;
            options.DetailedProfilingInfo->MountCacheWaitTime += mountCacheWaitTime;
            options.DetailedProfilingInfo->PermissionCacheWaitTime += permissionCacheWaitTime;
        }
    }

    TQueryOptions queryOptions;
    queryOptions.TimestampRange.Timestamp = options.Timestamp;
    queryOptions.TimestampRange.RetentionTimestamp = options.RetentionTimestamp;
    queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
    queryOptions.VerboseLogging = options.VerboseLogging;
    queryOptions.NewRangeInference = GetNativeConnection()->GetConfig()->DisableNewRangeInference
        ? false
        : options.NewRangeInference;
    queryOptions.ExecutionBackend = GetNativeConnection()->GetConfig()->UseWebAssembly
        ? static_cast<NCodegen::EExecutionBackend>(options.ExecutionBackend.value_or(NApi::EExecutionBackend::WebAssembly))
        : NCodegen::EExecutionBackend::Native;
    queryOptions.EnableCodeCache = options.EnableCodeCache;
    queryOptions.MaxSubqueries = options.MaxSubqueries;
    queryOptions.WorkloadDescriptor = options.WorkloadDescriptor;
    queryOptions.InputRowLimit = inputRowLimit;
    queryOptions.OutputRowLimit = outputRowLimit;
    queryOptions.AllowFullScan = options.AllowFullScan;
    queryOptions.ReadSessionId = readSessionId;
    queryOptions.MemoryLimitPerNode = options.MemoryLimitPerNode;
    queryOptions.ExecutionPool = options.ExecutionPool;
    queryOptions.Deadline = options.Timeout.value_or(Connection_->GetConfig()->DefaultSelectRowsTimeout).ToDeadLine();
    queryOptions.SuppressAccessTracking = options.SuppressAccessTracking;
    queryOptions.UseCanonicalNullRelations = options.UseCanonicalNullRelations;
    queryOptions.MergeVersionedRows = options.MergeVersionedRows;

    IUnversionedRowsetWriterPtr writer;
    TFuture<IUnversionedRowsetPtr> asyncRowset;
    std::tie(writer, asyncRowset) = CreateSchemafulRowsetWriter(query->GetTableSchema());

    auto statistics = WaitFor(queryExecutor->Execute(
        query,
        externalCGInfo,
        dataSource,
        writer,
        queryOptions))
        .ValueOrThrow();

    auto rowset = WaitFor(asyncRowset)
        .ValueOrThrow();

    if (options.FailOnIncompleteResult) {
        if (statistics.IncompleteInput) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::QueryInputRowCountLimitExceeded,
                "Query terminated prematurely due to excessive input; consider rewriting your query or changing input limit")
                << TErrorAttribute("input_row_limit", inputRowLimit);
        }
        if (statistics.IncompleteOutput) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::QueryOutputRowCountLimitExceeded,
                "Query terminated prematurely due to excessive output; consider rewriting your query or changing output limit")
                << TErrorAttribute("output_row_limit", outputRowLimit);
        }
    }

    return TSelectRowsResult{rowset, statistics};
}

NYson::TYsonString TClient::DoExplainQuery(
    const TString& queryString,
    const TExplainQueryOptions& options)
{
    auto parsedQuery = ParseSource(
        queryString,
        EParseMode::Query,
        /*placeholderValues*/ {},
        options.SyntaxVersion);

    auto cache = New<TStickyTableMountInfoCache>(Connection_->GetTableMountCache());
    TransformWithIndexStatement(&parsedQuery->AstHead, cache);

    auto udfRegistryPath = options.UdfRegistryPath
        ? *options.UdfRegistryPath
        : GetNativeConnection()->GetConfig()->UdfRegistryPath;

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());

        std::vector<TString> externalNames;
        for (const auto& name : names) {
            auto found = typeInferrers->find(name);
            if (found == typeInferrers->end()) {
                externalNames.push_back(name);
            }
        }

        auto descriptors = WaitFor(GetFunctionRegistry()->FetchFunctions(udfRegistryPath, externalNames))
            .ValueOrThrow();

        AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
    };

    auto queryPreparer = New<TQueryPreparer>(cache, GetNativeConnection()->GetInvoker());

    auto fragment = PreparePlanFragment(
        queryPreparer.Get(),
        *parsedQuery,
        fetchFunctions,
        QueryMemoryTracker_);

    return BuildExplainQueryYson(GetNativeConnection(), fragment, udfRegistryPath, options);
}

template <class T>
auto TClient::CallAndRetryIfMetadataCacheIsInconsistent(
    const TDetailedProfilingInfoPtr& profilingInfo,
    T&& callback) -> decltype(callback())
{
    int retryCount = 0;
    while (true) {
        TError error;

        try {
            return callback();
        } catch (const NYT::TErrorException& ex) {
            error = ex.Error();
        }

        const auto& config = Connection_->GetStaticConfig();
        const auto& tableMountCache = Connection_->GetTableMountCache();

        std::optional<TErrorCode> retryableErrorCode;
        TTabletInfoPtr tabletInfo;
        std::tie(retryableErrorCode, tabletInfo) = tableMountCache->InvalidateOnError(
            error,
            /*forceRetry*/ false);

        if (retryableErrorCode && ++retryCount <= config->TableMountCache->OnErrorRetryCount) {
            YT_LOG_DEBUG(error, "Got error, will retry (attempt %v of %v)",
                retryCount,
                config->TableMountCache->OnErrorRetryCount);
            auto now = Now();
            auto retryTime = (tabletInfo ? tabletInfo->UpdateTime : now) +
                config->TableMountCache->OnErrorSlackPeriod;
            if (retryTime > now) {
                TDelayedExecutor::WaitForDuration(retryTime - now);
            }
            if (profilingInfo) {
                profilingInfo->RetryReasons.push_back(*retryableErrorCode);
            }
            continue;
        }

        THROW_ERROR error;
    }
}

template <class TReq>
void TClient::ExecuteTabletServiceRequest(
    const TYPath& path,
    TStringBuf action,
    TReq* req)
{
    TTableId tableId;
    TCellTag externalCellTag;
    auto tableAttributes = NTableClient::ResolveExternalTable(
        MakeStrong(this),
        path,
        &tableId,
        &externalCellTag,
        {"path"});

    if (!IsTabletOwnerType(TypeFromId(tableId))) {
        THROW_ERROR_EXCEPTION("Object %v is not a tablet owner", path);
    }

    auto nativeCellTag = CellTagFromId(tableId);

    auto transactionAttributes = CreateEphemeralAttributes();
    transactionAttributes->Set(
        "title",
        Format("%v node %v", action, path));

    TTransactionStartOptions transactionOptions;
    transactionOptions.Attributes = std::move(transactionAttributes);
    transactionOptions.SuppressStartTimestampGeneration = true,
    transactionOptions.CoordinatorMasterCellTag = nativeCellTag;
    transactionOptions.ReplicateToMasterCellTags = TCellTagList{externalCellTag};
    transactionOptions.StartCypressTransaction = false;
    auto asyncTransaction = StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        transactionOptions);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    ToProto(req->mutable_table_id(), tableId);

    auto fullPath = tableAttributes->Get<TString>("path");
    SetDynamicTableCypressRequestFullPath(req, fullPath);

    auto actionData = MakeTransactionActionData(*req);

    auto nativeCellId = GetNativeConnection()->GetMasterCellId(nativeCellTag);
    auto externalCellId = GetNativeConnection()->GetMasterCellId(externalCellTag);
    transaction->AddAction(nativeCellId, actionData);
    if (nativeCellId != externalCellId) {
        transaction->AddAction(externalCellId, actionData);
    }

    WaitFor(transaction->Commit(TTransactionCommitOptions{
        .Force2PC = true,
        .CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy,
        .CellIdsToSyncWithBeforePrepare = {nativeCellId}
    }))
        .ThrowOnError();
}

void TClient::DoMountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    NTabletClient::NProto::TReqMount req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }
    if (options.CellId) {
        ToProto(req.mutable_cell_id(), options.CellId);
    }
    if (!options.TargetCellIds.empty()) {
        ToProto(req.mutable_target_cell_ids(), options.TargetCellIds);
    }
    req.set_freeze(options.Freeze);

    auto mountTimestamp = WaitFor(Connection_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();
    req.set_mount_timestamp(mountTimestamp);

    ExecuteTabletServiceRequest(path, "Mounting", &req);
}

void TClient::DoUnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    NTabletClient::NProto::TReqUnmount req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }
    req.set_force(options.Force);

    ExecuteTabletServiceRequest(path, "Unmounting", &req);
}

void TClient::DoRemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    NTabletClient::NProto::TReqRemount req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }

    ExecuteTabletServiceRequest(path, "Remounting", &req);
}

void TClient::DoFreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    NTabletClient::NProto::TReqFreeze req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }

    ExecuteTabletServiceRequest(path, "Freezing", &req);
}

void TClient::DoUnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    NTabletClient::NProto::TReqUnfreeze req;

    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }

    ExecuteTabletServiceRequest(path, "Unfreezing", &req);
}

NTabletClient::NProto::TReqReshard TClient::MakeReshardRequest(
    const TReshardTableOptions& options)
{
    NTabletClient::NProto::TReqReshard req;
    if (options.FirstTabletIndex) {
        req.set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req.set_last_tablet_index(*options.LastTabletIndex);
    }
    ToProto(req.mutable_trimmed_row_counts(), options.TrimmedRowCounts);

    return req;
}

TTableYPathProxy::TReqReshardPtr TClient::MakeYPathReshardRequest(
    const TYPath& path,
    const TReshardTableOptions& options)
{
    auto req = TTableYPathProxy::Reshard(path);
    SetMutationId(req, options);

    if (options.FirstTabletIndex) {
        req->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        req->set_last_tablet_index(*options.LastTabletIndex);
    }
    return req;
}

std::vector<TLegacyOwningKey> TClient::PickUniformPivotKeys(
    const TYPath& path,
    int tabletCount)
{
    if (tabletCount > MaxTabletCount) {
        THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
            MaxTabletCount);
    }

    auto proxy = CreateObjectServiceReadProxy(TMasterReadOptions());
    auto req = TTableYPathProxy::Get(path + "/@schema");
    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching table schema");

    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(rspOrError.Value()->value()));

    if (schema->Columns().empty()) {
        THROW_ERROR_EXCEPTION("Table schema is empty");
    }

    const auto& column = schema->Columns()[0];
    if (column.SortOrder() != ESortOrder::Ascending) {
        THROW_ERROR_EXCEPTION("Table is not sorted");
    }
    if (!column.IsOfV1Type()) {
        THROW_ERROR_EXCEPTION("First key column type %Qv is too complex",
            *column.LogicalType());
    }

    auto buildPivotKeys = [tabletCount] (i128 lo, i128 hi, bool isSigned)
    {
        TUnversionedOwningRowBuilder builder;
        std::vector<TLegacyOwningKey> pivots;
        pivots.push_back(builder.FinishRow());

        i128 span = hi - lo + 1;
        for (int i = 1; i < tabletCount; ++i) {
            i128 value = lo + (span * i) / tabletCount;
            if (isSigned) {
                builder.AddValue(MakeUnversionedInt64Value(static_cast<i64>(value)));
            } else {
                builder.AddValue(MakeUnversionedUint64Value(static_cast<ui64>(value)));
            }
            pivots.push_back(builder.FinishRow());
        }

        return pivots;
    };

    switch (column.CastToV1Type()) {
        case ESimpleLogicalValueType::Uint8:
            return buildPivotKeys(0, std::numeric_limits<ui8>::max(), false);
        case ESimpleLogicalValueType::Uint16:
            return buildPivotKeys(0, std::numeric_limits<ui16>::max(), false);
        case ESimpleLogicalValueType::Uint32:
            return buildPivotKeys(0, std::numeric_limits<ui32>::max(), false);
        case ESimpleLogicalValueType::Uint64:
            return buildPivotKeys(0, std::numeric_limits<ui64>::max(), false);

        case ESimpleLogicalValueType::Int8:
            return buildPivotKeys(std::numeric_limits<i8>::min(), std::numeric_limits<i8>::max(), true);
        case ESimpleLogicalValueType::Int16:
            return buildPivotKeys(std::numeric_limits<i16>::min(), std::numeric_limits<i16>::max(), true);
        case ESimpleLogicalValueType::Int32:
            return buildPivotKeys(std::numeric_limits<int>::min(), std::numeric_limits<int>::max(), true);
        case ESimpleLogicalValueType::Int64:
            return buildPivotKeys(std::numeric_limits<i64>::min(), std::numeric_limits<i64>::max(), true);

        default:
            THROW_ERROR_EXCEPTION("First key column has improper type: expected "
                "integral, got %Qv",
                *column.LogicalType());
    }
}

void TClient::DoReshardTableWithPivotKeys(
    const TYPath& path,
    const std::vector<TLegacyOwningKey>& pivotKeys,
    const TReshardTableOptions& options)
{
    auto req = MakeReshardRequest(options);
    ToProto(req.mutable_pivot_keys(), pivotKeys);
    req.set_tablet_count(pivotKeys.size());

    ExecuteTabletServiceRequest(path, "Resharding", &req);
}

void TClient::DoReshardTableWithTabletCount(
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options)
{
    if (options.Uniform.value_or(false)) {
        if (options.FirstTabletIndex || options.LastTabletIndex) {
            THROW_ERROR_EXCEPTION("Tablet indices cannot be specified for uniform reshard");
        }

        auto pivots = PickUniformPivotKeys(path, tabletCount);
        DoReshardTableWithPivotKeys(path, pivots, options);
        return;
    }

    if (options.EnableSlicing.value_or(false)) {
        auto pivots = PickPivotKeysWithSlicing(
            MakeStrong(this),
            path,
            tabletCount,
            options,
            Logger);
        DoReshardTableWithPivotKeys(path, pivots, options);
        return;
    }

    auto req = MakeReshardRequest(options);
    req.set_tablet_count(tabletCount);

    ExecuteTabletServiceRequest(path, "Resharding", &req);
}

std::vector<TTabletActionId> TClient::DoReshardTableAutomatic(
    const TYPath& path,
    const TReshardTableAutomaticOptions& options)
{
    if (options.FirstTabletIndex || options.LastTabletIndex) {
        THROW_ERROR_EXCEPTION("Tablet indices cannot be specified for automatic reshard");
    }

    TTableId tableId;
    TCellTag externalCellTag;
    auto attributes =  NTableClient::ResolveExternalTable(
        MakeStrong(this),
        path,
        &tableId,
        &externalCellTag,
        {"tablet_cell_bundle", "dynamic"});

    if (TypeFromId(tableId) != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, got %Qlv",
            path,
            EObjectType::Table,
            TypeFromId(tableId));
    }

    if (!attributes->Get<bool>("dynamic")) {
        THROW_ERROR_EXCEPTION("Table %v must be dynamic",
            path);
    }

    auto bundle = attributes->Get<TString>("tablet_cell_bundle");
    ValidatePermissionImpl("//sys/tablet_cell_bundles/" + ToYPathLiteral(bundle), EPermission::Use);

    auto req = TTableYPathProxy::ReshardAutomatic(FromObjectId(tableId));
    SetMutationId(req, options);
    req->set_keep_actions(options.KeepActions);
    auto proxy = CreateObjectServiceWriteProxy(externalCellTag);
    auto protoRsp = WaitFor(proxy.Execute(req))
        .ValueOrThrow();
    return FromProto<std::vector<TTabletActionId>>(protoRsp->tablet_actions());
}

void TClient::DoAlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    auto req = TTableYPathProxy::Alter(path);
    SetTransactionId(req, options, true);
    SetMutationId(req, options);

    if (options.Schema) {
        ToProto(req->mutable_schema(), *options.Schema);
    }
    if (options.SchemaId) {
        ToProto(req->mutable_schema_id(), *options.SchemaId);
    }
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }
    if (options.SchemaModification) {
        req->set_schema_modification(static_cast<int>(*options.SchemaModification));
    }
    if (options.ReplicationProgress) {
        ToProto(req->mutable_replication_progress(), *options.ReplicationProgress);
    }

    auto proxy = CreateObjectServiceWriteProxy();
    WaitFor(proxy.Execute(req))
        .ThrowOnError();
}

void TClient::DoTrimTable(
    const TYPath& path,
    int tabletIndex,
    i64 trimmedRowCount,
    const TTrimTableOptions& /*options*/)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateOrdered();

    const auto& permissionCache = Connection_->GetPermissionCache();
    NSecurityClient::TPermissionKey permissionKey{
        .Object = FromObjectId(tableInfo->TableId),
        .User = Options_.GetAuthenticatedUser(),
        .Permission = NYTree::EPermission::Write
    };
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError();

    auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);

    auto channel = GetCellChannelOrThrow(tabletInfo->CellId);

    TTabletServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Connection_->GetConfig()->DefaultTrimTableTimeout);

    auto req = proxy.Trim();
    ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
    req->set_mount_revision(tabletInfo->MountRevision);
    req->set_trimmed_row_count(trimmedRowCount);

    WaitFor(req->Invoke())
        .ValueOrThrow();
}

void TClient::DoAlterTableReplica(
    TTableReplicaId replicaId,
    const TAlterTableReplicaOptions& options)
{
    for (const auto& handler : TypeHandlers_) {
        if (auto result = handler->AlterTableReplica(replicaId, options)) {
            return;
        }
    }

    THROW_ERROR_EXCEPTION("Unsupported object type %Qlv", TypeFromId(replicaId));
}

TYsonString TClient::DoGetTablePivotKeys(
    const NYPath::TYPath& path,
    const TGetTablePivotKeysOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    auto keySchema = tableInfo->Schemas[ETableSchemaKind::Primary]->ToKeys();

    auto serializePivotKey = [&] (TFluentList fluent, const TTabletInfoPtr& tablet) {
        const auto& key = tablet->PivotKey;
        if (options.RepresentKeyAsList) {
            fluent
                .Item().DoListFor(key, [&] (TFluentList fluent, const TUnversionedValue& value) {
                    fluent
                        .Item().Value(value);
                });
        } else {
            fluent
                .Item().DoMapFor(key, [&] (TFluentMap fluent, const TUnversionedValue& value) {
                    YT_VERIFY(value.Id < keySchema->GetColumnCount());
                    fluent
                        .Item(keySchema->Columns()[value.Id].Name()).Value(value);
                });
        }
    };

    return BuildYsonStringFluently()
        .DoListFor(tableInfo->Tablets, serializePivotKey);
}

void TClient::DoCreateTableBackup(
    const TBackupManifestPtr& manifest,
    const TCreateTableBackupOptions& options)
{
    TBackupSession session(manifest, MakeStrong(this), options, Logger);
    session.RunCreate();
}

void TClient::DoRestoreTableBackup(
    const TBackupManifestPtr& manifest,
    const TRestoreTableBackupOptions& options)
{
    TBackupSession session(manifest, MakeStrong(this), options, Logger);
    session.RunRestore();
}

std::vector<TTabletActionId> TClient::DoBalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    ValidatePermissionImpl("//sys/tablet_cell_bundles/" + ToYPathLiteral(tabletCellBundle), EPermission::Use);

    std::vector<TFuture<TTabletCellBundleYPathProxy::TRspBalanceTabletCellsPtr>> cellResponses;

    if (movableTables.empty()) {
        auto cellTags = Connection_->GetSecondaryMasterCellTags();
        cellTags.push_back(Connection_->GetPrimaryMasterCellTag());
        auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
        SetMutationId(req, options);
        req->set_keep_actions(options.KeepActions);
        for (const auto& cellTag : cellTags) {
            auto proxy = CreateObjectServiceWriteProxy(cellTag);
            cellResponses.push_back(proxy.Execute(req));
        }
    } else {
        THashMap<TCellTag, std::vector<TTableId>> tablesByCells;

        for (const auto& path : movableTables) {
            TTableId tableId;
            TCellTag externalCellTag;
            auto attributes =  NTableClient::ResolveExternalTable(
                MakeStrong(this),
                path,
                &tableId,
                &externalCellTag,
                {"dynamic", "tablet_cell_bundle"});

            if (TypeFromId(tableId) != EObjectType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid type of %v: expected %Qlv, got %Qlv",
                    path,
                    EObjectType::Table,
                    TypeFromId(tableId));
            }

            if (!attributes->Get<bool>("dynamic")) {
                THROW_ERROR_EXCEPTION("Table %v must be dynamic", path);
            }

            auto actualBundle = attributes->Find<TString>("tablet_cell_bundle");
            if (!actualBundle || *actualBundle != tabletCellBundle) {
                THROW_ERROR_EXCEPTION("All tables must be from the tablet cell bundle %Qv", tabletCellBundle);
            }

            tablesByCells[externalCellTag].push_back(tableId);
        }

        for (const auto& [cellTag, tableIds] : tablesByCells) {
            auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
            req->set_keep_actions(options.KeepActions);
            SetMutationId(req, options);
            ToProto(req->mutable_movable_tables(), tableIds);
            auto proxy = CreateObjectServiceWriteProxy(cellTag);
            cellResponses.push_back(proxy.Execute(req));
        }
    }

    std::vector<TTabletActionId> tabletActions;
    for (auto& future : cellResponses) {
        auto errorOrRsp = WaitFor(future);
        if (errorOrRsp.IsOK()) {
            const auto& rsp = errorOrRsp.Value();
            auto tabletActionsFromCell = FromProto<std::vector<TTabletActionId>>(rsp->tablet_actions());
            tabletActions.insert(tabletActions.end(), tabletActionsFromCell.begin(), tabletActionsFromCell.end());
        } else {
            YT_LOG_DEBUG(errorOrRsp, "Tablet cell balancing subrequest failed");
        }
    }

    return tabletActions;
}

IQueueRowsetPtr TClient::DoPullQueueImpl(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options,
    bool checkPermissions)
{
    // Bypassing authentication is only possible when using the native tablet node api.
    YT_VERIFY(checkPermissions || options.UseNativeTabletNodeApi);

    THROW_ERROR_EXCEPTION_IF(
        offset < 0,
        "Cannot read table %v at a negative offset %v",
        queuePath,
        offset);

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(queuePath.GetPath()))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateOrdered();

    // The non-native API via SelectRows checks permissions on its own.
    if (checkPermissions && options.UseNativeTabletNodeApi) {
        NSecurityClient::TPermissionKey permissionKey{
            .Object = FromObjectId(tableInfo->TableId),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Read,
        };
        const auto& permissionCache = Connection_->GetPermissionCache();
        WaitFor(permissionCache->Get(permissionKey))
            .ThrowOnError();
    }

    // The code below is used to facilitate reading from [chaos] replicated tables and
    // performing sync reads from async chaos replicas.

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];

    if (options.FallbackReplicaId && tableInfo->UpstreamReplicaId != options.FallbackReplicaId) {
        THROW_ERROR_EXCEPTION("Invalid upstream replica id for chosen sync replica %Qv: expected %v, got %v",
            queuePath,
            options.FallbackReplicaId,
            tableInfo->UpstreamReplicaId);
    }

    if (options.FallbackTableSchema && tableInfo->ReplicationCardId) {
        ValidateTableSchemaUpdateInternal(
            *options.FallbackTableSchema,
            *tableInfo->Schemas[ETableSchemaKind::Primary],
            GetSchemaUpdateEnabledFeatures(),
            true,
            false);
    }

    // The non-native API via SelectRows redirects requests to fallback-replicas on its own.
    if ((tableInfo->IsReplicated() ||
        tableInfo->IsChaosReplicated() ||
        (tableInfo->ReplicationCardId && options.ReplicaConsistency == EReplicaConsistency::Sync)) &&
        options.UseNativeTabletNodeApi)
    {

        auto bannedReplicaTracker = static_cast<bool>(tableInfo->ReplicationCardId) || tableInfo->IsChaosReplicated()
            ? Connection_->GetBannedReplicaTrackerCache()->GetTracker(tableInfo->TableId)
            : nullptr;

        auto pickedSyncReplicas = PrepareInSyncReplicaCandidates(options, {tableInfo}).second[0];

        auto retryCountLimit = tableInfo->ReplicationCardId
            ? Connection_->GetConfig()->ReplicaFallbackRetryCount
            : 0;

        TErrorOr<IQueueRowsetPtr> resultOrError;

        for (int retryCount = 0; retryCount <= retryCountLimit; ++retryCount) {
            TTableReplicaInfoPtrList inSyncReplicas;
            std::vector<TTableReplicaId> bannedSyncReplicaIds;
            for (const auto& replicaInfo : pickedSyncReplicas) {
                if (bannedReplicaTracker && bannedReplicaTracker->IsReplicaBanned(replicaInfo->ReplicaId)) {
                    bannedSyncReplicaIds.push_back(replicaInfo->ReplicaId);
                } else {
                    inSyncReplicas.push_back(replicaInfo);
                }
            }

            if (inSyncReplicas.empty()) {
                std::vector<TError> replicaErrors;
                for (auto bannedReplicaId : bannedSyncReplicaIds) {
                    if (auto error = bannedReplicaTracker->GetReplicaError(bannedReplicaId); !error.IsOK()) {
                        replicaErrors.push_back(std::move(error));
                    }
                }

                auto error = TError(
                    NTabletClient::EErrorCode::NoInSyncReplicas,
                    "No in-sync replicas found for table %v",
                    tableInfo->Path)
                    << TErrorAttribute("banned_replicas", bannedSyncReplicaIds);
                *error.MutableInnerErrors() = std::move(replicaErrors);
                THROW_ERROR error;
            }

            auto replicaFallbackInfo = GetReplicaFallbackInfo(inSyncReplicas);
            replicaFallbackInfo.OriginalTableSchema = schema;

            auto unresolveOptions = options;
            unresolveOptions.ReplicaConsistency = EReplicaConsistency::None;
            unresolveOptions.FallbackTableSchema = replicaFallbackInfo.OriginalTableSchema;
            unresolveOptions.FallbackReplicaId = replicaFallbackInfo.ReplicaId;
            resultOrError = WaitFor(replicaFallbackInfo.Client->PullQueue(
                replicaFallbackInfo.Path,
                offset,
                partitionIndex,
                rowBatchReadOptions,
                unresolveOptions));
            if (resultOrError.IsOK()) {
                return resultOrError.Value();
            }

            YT_LOG_DEBUG(
                resultOrError,
                "Fallback to replica failed (ReplicaId: %v)",
                replicaFallbackInfo.ReplicaId);

            if (bannedReplicaTracker) {
                bannedReplicaTracker->BanReplica(replicaFallbackInfo.ReplicaId, resultOrError.Truncate());
            }
        }

        YT_VERIFY(!resultOrError.IsOK());
        resultOrError.ThrowOnError();
    } else if (tableInfo->IsReplicationLog()) {
        THROW_ERROR_EXCEPTION("This query is not supported for replication log tables");
    }

    // The code below performs the actual request.
    auto adjustedRowBatchReadOptions = rowBatchReadOptions;
    adjustedRowBatchReadOptions.MaxRowCount = ComputeRowsToRead(rowBatchReadOptions);

    IUnversionedRowsetPtr rowset;
    if (options.UseNativeTabletNodeApi) {
        rowset = DoPullQueueViaTabletNodeApi(
            queuePath,
            offset,
            partitionIndex,
            adjustedRowBatchReadOptions,
            options,
            checkPermissions);
    } else {
        rowset = DoPullQueueViaSelectRows(
            queuePath,
            offset,
            partitionIndex,
            adjustedRowBatchReadOptions,
            options);
    }

    auto startOffset = offset;
    if (!rowset->GetRows().Empty()) {
        startOffset = GetStartOffset(rowset);
    }

    return CreateQueueRowset(rowset, startOffset);
}

IQueueRowsetPtr TClient::DoPullQueue(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options)
{
    return DoPullQueueImpl(
        queuePath,
        offset,
        partitionIndex,
        rowBatchReadOptions,
        options,
        /*checkPermissions*/ true);
}

IQueueRowsetPtr TClient::DoPullQueueUnauthenticated(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options)
{
    return DoPullQueueImpl(
        queuePath,
        offset,
        partitionIndex,
        rowBatchReadOptions,
        options,
        /*checkPermissions*/ false);
}

IUnversionedRowsetPtr TClient::DoPullQueueViaSelectRows(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options)
{
    auto rowsToRead = rowBatchReadOptions.MaxRowCount;

    auto readResult = DoSelectRows(
        Format(
            "* from [%v] where [$tablet_index] = %v and [$row_index] between %v and %v",
            queuePath.GetPath(),
            partitionIndex,
            offset,
            offset + rowsToRead - 1),
        options);

    if (readResult.Rowset->GetRows().Empty()) {
        readResult = DoSelectRows(
            Format(
                "* from [%v] where [$tablet_index] = %v and [$row_index] >= %v limit %v",
                queuePath.GetPath(),
                partitionIndex,
                offset,
                rowsToRead),
            options);
    }

    return readResult.Rowset;
}

IUnversionedRowsetPtr TClient::DoPullQueueViaTabletNodeApi(
    const NYPath::TRichYPath& queuePath,
    i64 offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullQueueOptions& options,
    bool checkPermissions)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(queuePath.GetPath()))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateOrdered();

    if (checkPermissions) {
        NSecurityClient::TPermissionKey permissionKey{
            .Object = FromObjectId(tableInfo->TableId),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Read,
        };
        const auto& permissionCache = Connection_->GetPermissionCache();
        WaitFor(permissionCache->Get(permissionKey))
            .ThrowOnError();
    }

    auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(partitionIndex);

    auto channel = GetReadCellChannelOrThrow(tabletInfo->CellId);
    TQueryServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultFetchTableRowsTimeout));

    auto req = proxy.FetchTableRows();
    ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
    ToProto(req->mutable_cell_id(), tabletInfo->CellId);
    req->set_mount_revision(tabletInfo->MountRevision);
    req->set_tablet_index(partitionIndex);
    req->set_row_index(offset);
    req->set_max_row_count(rowBatchReadOptions.MaxRowCount);
    req->set_max_data_weight(rowBatchReadOptions.MaxDataWeight);
    ToProto(req->mutable_options()->mutable_workload_descriptor(), options.WorkloadDescriptor);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    struct TPullQueueTag {};
    auto responseData = MergeRefsToRef<TPullQueueTag>(rsp->Attachments());
    auto reader = CreateWireProtocolReader(responseData, New<TRowBuffer>(TPullQueueTag{}));
    std::vector<TUnversionedRow> rows;
    while (!reader->IsFinished()) {
        auto batch = reader->ReadUnversionedRowset(/*captureValues*/ true);
        rows.reserve(rows.size() + batch.size());
        rows.insert(rows.end(), batch.begin(), batch.end());
    }

    return CreateRowset(
        tableInfo->Schemas[ETableSchemaKind::Query],
        MakeSharedRange(rows, reader->GetRowBuffer()));
}

IQueueRowsetPtr TClient::DoPullConsumer(
    const NYPath::TRichYPath& consumerPath,
    const NYPath::TRichYPath& queuePath,
    std::optional<i64> offset,
    int partitionIndex,
    const TQueueRowBatchReadOptions& rowBatchReadOptions,
    const TPullConsumerOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto consumerTableInfo = WaitFor(tableMountCache->GetTableInfo(consumerPath.GetPath()))
        .ValueOrThrow();

    NSecurityClient::TPermissionKey permissionKey{
        .Object = FromObjectId(consumerTableInfo->TableId),
        .User = Options_.GetAuthenticatedUser(),
        .Permission = EPermission::Read,
    };
    const auto& permissionCache = Connection_->GetPermissionCache();
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError();

    auto registrationCheckResult = Connection_->GetQueueConsumerRegistrationManager()->GetRegistration(queuePath, consumerPath);
    if (!registrationCheckResult.Registration) {
        THROW_ERROR_EXCEPTION(
            NYT::NSecurityClient::EErrorCode::AuthorizationError,
            "Consumer %v is not registered for queue %v",
            registrationCheckResult.ResolvedConsumer,
            registrationCheckResult.ResolvedQueue)
            << TErrorAttribute("raw_queue", queuePath)
            << TErrorAttribute("raw_consumer", consumerPath);
    }

    IClientPtr queueClusterClient = MakeStrong(this);
    if (auto queueCluster = queuePath.GetCluster()) {
        auto queueClusterConnection = FindRemoteConnection(Connection_, *queueCluster);
        if (!queueClusterConnection) {
            THROW_ERROR_EXCEPTION(
                "Queue cluster %Qv was not found for path %v",
                *queueCluster,
                queuePath
            );
        }

        auto queueClientOptions = TClientOptions::FromUser(Options_.GetAuthenticatedUser());

        queueClusterClient = queueClusterConnection->CreateNativeClient(queueClientOptions);
        YT_VERIFY(queueClusterClient);
    }

    i64 resultOffset = 0;
    if (offset) {
        resultOffset = *offset;
    } else {
        // PullConsumer is supported only for consumers from current cluster.
        IClientPtr consumerClusterClient = MakeStrong(this);

        auto subConsumerClient = CreateSubConsumerClient(consumerClusterClient, queueClusterClient, consumerPath.GetPath(), queuePath);
        auto partitions = WaitFor(subConsumerClient->CollectPartitions(std::vector<int>{partitionIndex}))
            .ValueOrThrow();
        if (partitions.size() < 1) {
            YT_LOG_DEBUG(
                "Consumer partition was not found during offset calculation (PartitionIndex: %v, ConsumerPath: %v, QueuePath: %v)",
                partitionIndex,
                consumerPath,
                queuePath

            );
            THROW_ERROR_EXCEPTION(
                "Failed to calculate current offset for consumer %v for queue %v",
                consumerPath,
                queuePath
            );
        }
        resultOffset = partitions[0].NextRowIndex;
    }

    TPullQueueOptions pullQueueOptions = options;
    pullQueueOptions.UseNativeTabletNodeApi = true;

    return WaitFor(DynamicPointerCast<IInternalClient>(queueClusterClient)->PullQueueUnauthenticated(
        queuePath,
        resultOffset,
        partitionIndex,
        rowBatchReadOptions,
        pullQueueOptions))
        .ValueOrThrow();
}

void TClient::DoRegisterQueueConsumer(
    const NYPath::TRichYPath& queuePath,
    const NYPath::TRichYPath& consumerPath,
    bool vital,
    const TRegisterQueueConsumerOptions& options)
{
    auto queueConnection = FindRemoteConnection(Connection_, queuePath.GetCluster());
    const auto& queueTableMountCache = queueConnection->GetTableMountCache();
    auto queueTableInfo = WaitFor(queueTableMountCache->GetTableInfo(queuePath.GetPath()))
        .ValueOrThrow();

    NSecurityClient::TPermissionKey permissionKey{
        .Object = FromObjectId(queueTableInfo->TableId),
        .User = Options_.GetAuthenticatedUser(),
        .Permission = EPermission::RegisterQueueConsumer,
        .Vital = vital,
    };
    const auto& permissionCache = queueConnection->GetPermissionCache();
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError();

    auto registrationCache = Connection_->GetQueueConsumerRegistrationManager();
    registrationCache->RegisterQueueConsumer(queuePath, consumerPath, vital, options.Partitions);

    YT_LOG_DEBUG(
        "Registered queue consumer (Queue: %v, Consumer: %v, Vital: %v, Partitions: %v)",
        queuePath,
        consumerPath,
        vital,
        options.Partitions);
}

void TClient::DoUnregisterQueueConsumer(
    const NYPath::TRichYPath& queuePath,
    const NYPath::TRichYPath& consumerPath,
    const TUnregisterQueueConsumerOptions& /*options*/)
{
    auto queueConnection = FindRemoteConnection(Connection_, queuePath.GetCluster());
    const auto& tableMountCache = queueConnection->GetTableMountCache();
    auto queueTableInfoOrError = WaitFor(tableMountCache->GetTableInfo(queuePath.GetPath()));

    auto consumerConnection = FindRemoteConnection(Connection_, consumerPath.GetCluster());
    auto consumerTableInfoOrError = WaitFor(consumerConnection->GetTableMountCache()->GetTableInfo(consumerPath.GetPath()));

    // NB: We cannot really check permissions if the objects are not available anymore.
    // For now, we will allow anyone to delete registrations with nonexistent queues/consumers.
    if (queueTableInfoOrError.IsOK() && consumerTableInfoOrError.IsOK()) {
        const auto& queueTableInfo = queueTableInfoOrError.Value();
        const auto& consumerTableInfo = consumerTableInfoOrError.Value();

        NSecurityClient::TPermissionKey queuePermissionKey{
            .Object = FromObjectId(queueTableInfo->TableId),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Remove,
        };

        NSecurityClient::TPermissionKey consumerPermissionKey{
            .Object = FromObjectId(consumerTableInfo->TableId),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Remove,
        };

        WaitFor(AnySucceeded(std::vector{
            queueConnection->GetPermissionCache()->Get(queuePermissionKey),
            consumerConnection->GetPermissionCache()->Get(consumerPermissionKey)
        }))
            .ThrowOnError();
    }

    auto registrationCache = Connection_->GetQueueConsumerRegistrationManager();
    registrationCache->UnregisterQueueConsumer(queuePath, consumerPath);

    YT_LOG_DEBUG("Unregistered queue consumer (Queue: %v, Consumer: %v)", queuePath, consumerPath);
}

std::vector<TListQueueConsumerRegistrationsResult> TClient::DoListQueueConsumerRegistrations(
    const std::optional<TRichYPath>& queuePath,
    const std::optional<TRichYPath>& consumerPath,
    const TListQueueConsumerRegistrationsOptions& /*options*/)
{
    auto registrationCache = Connection_->GetQueueConsumerRegistrationManager();
    auto registrations = registrationCache->ListRegistrations(queuePath, consumerPath);

    std::vector<TListQueueConsumerRegistrationsResult> result;
    result.reserve(registrations.size());
    for (const auto& registration : registrations) {
        result.push_back({
            .QueuePath = registration.Queue,
            .ConsumerPath = registration.Consumer,
            .Vital = registration.Vital,
            .Partitions = registration.Partitions,
        });
    }

    return result;
}

TSyncAlienCellsResult TClient::DoSyncAlienCells(
    const std::vector<TAlienCellDescriptorLite>& alienCellDescriptors,
    const TSyncAlienCellOptions& options)
{
    auto channel = GetMasterChannelOrThrow(options.ReadFrom, PrimaryMasterCellTagSentinel);
    auto proxy = TChaosMasterServiceProxy(channel);
    proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultSyncAlienCellsTimeout));
    auto req = proxy.SyncAlienCells();

    ToProto(req->mutable_cell_descriptors(), alienCellDescriptors);
    req->set_full_sync(options.FullSync);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return {
        FromProto<std::vector<TAlienCellDescriptor>>(rsp->cell_descriptors()),
        rsp->enable_metadata_cells()
    };
}

class TTabletPullRowsSession
    : public TRefCounted
{
public:
    struct TTabletRequest
    {
        int TabletIndex;
        std::optional<i64> StartReplicationRowIndex;
        TReplicationProgress Progress;
    };

    TTabletPullRowsSession(
        IClientPtr client,
        TTableSchemaPtr schema,
        int timestampColumnIndex,
        TTabletInfoPtr tabletInfo,
        bool versioned,
        const TPullRowsOptions& options,
        TTabletRequest request,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
    : Client_(std::move(client))
    , Schema_(std::move(schema))
    , TimestampColumnIndex_(timestampColumnIndex)
    , TabletInfo_(std::move(tabletInfo))
    , Versioned_(versioned)
    , Options_(options)
    , Request_(std::move(request))
    , Invoker_(std::move(invoker))
    , ReplicationProgress_(std::move(Request_.Progress))
    , ReplicationRowIndex_(Request_.StartReplicationRowIndex)
    , Logger(logger
        .WithTag("TabletId: %v", TabletInfo_->TabletId))
    { }

    TFuture<void> RunRequest()
    {
        return DoPullRows();
    }

    TTabletInfoPtr GetTabletInfo() const
    {
        return TabletInfo_;
    }

    const TReplicationProgress& GetReplicationProgress() const
    {
        return ReplicationProgress_;
    }

    std::optional<i64> GetEndReplicationRowIndex() const
    {
        return ReplicationRowIndex_;
    }

    i64 GetRowCount() const
    {
        return RowCount_;
    }

    i64 GetDataWeight() const
    {
        return DataWeight_;
    }

    std::vector<TTypeErasedRow> GetRows(TTimestamp maxTimestamp, const TRowBufferPtr& outputRowBuffer)
    {
        return DoGetRows(maxTimestamp, outputRowBuffer);
    }

    const TErrorOr<TQueryServiceProxy::TRspPullRowsPtr>& GetResultOrError() const
    {
        return ResultOrError_;
    }

private:
    const IClientPtr Client_;
    const TTableSchemaPtr Schema_;
    const int TimestampColumnIndex_;
    const TTabletInfoPtr TabletInfo_;
    const bool Versioned_;
    const TPullRowsOptions& Options_;
    const TTabletRequest Request_;
    const IInvokerPtr Invoker_;

    TErrorOr<TQueryServiceProxy::TRspPullRowsPtr> ResultOrError_;

    TReplicationProgress ReplicationProgress_;
    std::optional<i64> ReplicationRowIndex_;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    const NLogging::TLogger Logger;

    TFuture<void> DoPullRows()
    {
        const auto& connection = Client_->GetNativeConnection();
        const auto& cellDirectory = connection->GetCellDirectory();
        const auto& networks = connection->GetNetworks();
        auto channel = CreateTabletReadChannel(
            Client_->GetChannelFactory(),
            *cellDirectory->GetDescriptorByCellIdOrThrow(TabletInfo_->CellId),
            Options_,
            networks);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Options_.Timeout.value_or(connection->GetConfig()->DefaultPullRowsTimeout));
        auto req = proxy.PullRows();
        req->set_request_codec(static_cast<int>(connection->GetConfig()->LookupRowsRequestCodec));
        req->set_response_codec(static_cast<int>(connection->GetConfig()->LookupRowsResponseCodec));
        req->set_mount_revision(TabletInfo_->MountRevision);
        req->set_max_rows_per_read(Options_.TabletRowsPerRead);
        req->set_upper_timestamp(Options_.UpperTimestamp);
        ToProto(req->mutable_tablet_id(), TabletInfo_->TabletId);
        ToProto(req->mutable_cell_id(), TabletInfo_->CellId);
        ToProto(req->mutable_start_replication_progress(), ReplicationProgress_);
        ToProto(req->mutable_upstream_replica_id(), Options_.UpstreamReplicaId);
        if (ReplicationRowIndex_.has_value()) {
            req->set_start_replication_row_index(*ReplicationRowIndex_);
        }

        YT_LOG_DEBUG("Issuing pull rows request (Progress: %v, StartRowIndex: %v)",
            ReplicationProgress_,
            ReplicationRowIndex_);

        return req->Invoke()
            .Apply(BIND(&TTabletPullRowsSession::OnPullRowsResponse, MakeWeak(this))
                .AsyncVia(Invoker_));
    }

    void OnPullRowsResponse(const TErrorOr<TQueryServiceProxy::TRspPullRowsPtr>& resultOrError)
    {
        ResultOrError_ = resultOrError;
        if (!resultOrError.IsOK()) {
            YT_LOG_DEBUG(resultOrError, "Pull rows request failed");
            return;
        }

        const auto& result = resultOrError.Value();
        ReplicationProgress_ = FromProto<TReplicationProgress>(result->end_replication_progress());
        if (result->has_end_replication_row_index()) {
            ReplicationRowIndex_ = result->end_replication_row_index();
        }
        DataWeight_ += result->data_weight();
        RowCount_ += result->row_count();

        YT_LOG_DEBUG("Got pull rows response (RowCount: %v, DataWeight: %v, EndReplicationRowIndex: %v, Progress: %v)",
            result->row_count(),
            result->data_weight(),
            ReplicationRowIndex_,
            ReplicationProgress_);
    }

    std::vector<TTypeErasedRow> DoGetRows(TTimestamp maxTimestamp, const TRowBufferPtr& outputRowBuffer)
    {
        if (!ResultOrError_.IsOK()) {
            return {};
        }

        auto* responseCodec = NCompression::GetCodec(Client_->GetNativeConnection()->GetConfig()->LookupRowsResponseCodec);
        auto responseData = responseCodec->Decompress(ResultOrError_.Value()->Attachments()[0]);
        auto reader = CreateWireProtocolReader(responseData, outputRowBuffer);
        auto resultSchemaData = IWireProtocolReader::GetSchemaData(*Schema_, TColumnFilter());

        if (Versioned_) {
            return ReadVersionedRows(maxTimestamp, std::move(reader), resultSchemaData);
        } else {
            return ReadUnversionedRows(maxTimestamp, std::move(reader), resultSchemaData);
        }
    }

    std::vector<TTypeErasedRow> ReadVersionedRows(
        TTimestamp maxTimestamp,
        std::unique_ptr<IWireProtocolReader> reader,
        const TSchemaData& schemaData)
    {
        std::vector<TTypeErasedRow> rows;
        while (!reader->IsFinished()) {
            auto row = reader->ReadVersionedRow(schemaData, true);
            if (ExtractTimestampFromPulledRow(row) > maxTimestamp) {
                ReplicationRowIndex_.reset();
                break;
            }
            rows.push_back(row.ToTypeErasedRow());
        }
        return rows;
    }

    std::vector<TTypeErasedRow> ReadUnversionedRows(
        TTimestamp maxTimestamp,
        std::unique_ptr<IWireProtocolReader> reader,
        const TSchemaData& schemaData)
    {
        std::vector<TTypeErasedRow> rows;
        while (!reader->IsFinished()) {
            auto row = reader->ReadSchemafulRow(schemaData, true);
            if (FromUnversionedValue<ui64>(row[TimestampColumnIndex_]) > maxTimestamp) {
                ReplicationRowIndex_.reset();
                break;
            }
            rows.push_back(row.ToTypeErasedRow());
        }
        return rows;
    }
};

TPullRowsResult TClient::DoPullRows(
    const TYPath& path,
    const TPullRowsOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    const auto& schema = tableInfo->Schemas[ETableSchemaKind::VersionedWrite];

    if (options.TableSchema) {
        ValidateTableSchemaUpdateInternal(
            *tableInfo->Schemas[ETableSchemaKind::Primary],
            *options.TableSchema,
            GetSchemaUpdateEnabledFeatures(),
            true,
            false);
    }

    auto& segments = options.ReplicationProgress.Segments;
    if (segments.empty()) {
        THROW_ERROR_EXCEPTION("Invalid replication progress: no segments");
    }
    if (segments.size() > 1 && options.OrderRowsByTimestamp) {
        THROW_ERROR_EXCEPTION("Invalid replication progress: more than one segment while ordering by timestamp requested");
    }

    YT_LOG_DEBUG("Pulling rows (OrderedByTimestamp: %v, UpperTimestamp: %v, Progress: %v, StartRowIndexes: %v, Sorted: %v)",
        options.OrderRowsByTimestamp,
        options.UpperTimestamp,
        options.ReplicationProgress,
        options.StartReplicationRowIndexes,
        tableInfo->IsSorted());

    auto getStartReplicationRowIndex = [&] (int tabletIndex) -> std::optional<i64> {
        const auto& tabletInfo = tableInfo->Tablets[tabletIndex];
        if (auto it = options.StartReplicationRowIndexes.find(tabletInfo->TabletId)) {
            return it->second;
        }
        return {};
    };

    int timestampColumnIndex = 0;
    std::vector<TTabletPullRowsSession::TTabletRequest> requests;
    if (tableInfo->IsSorted()) {
        int startIndex = tableInfo->GetTabletIndexForKey(segments[0].LowerKey.Get());
        std::vector<TUnversionedRow> pivotKeys{segments[0].LowerKey.Get()};

        for (int index = startIndex + 1; index < std::ssize(tableInfo->Tablets) && tableInfo->Tablets[index]->PivotKey < options.ReplicationProgress.UpperKey; ++index) {
            pivotKeys.push_back(tableInfo->Tablets[index]->PivotKey.Get());
        }
        auto progresses = ScatterReplicationProgress(options.ReplicationProgress, pivotKeys, options.ReplicationProgress.UpperKey.Get());
        YT_VERIFY(progresses.size() == pivotKeys.size());

        for (int index = startIndex; index < std::ssize(tableInfo->Tablets) && tableInfo->Tablets[index]->PivotKey < options.ReplicationProgress.UpperKey; ++index) {
            requests.push_back({
                .TabletIndex = index,
                .StartReplicationRowIndex = getStartReplicationRowIndex(index),
                .Progress = std::move(progresses[index - startIndex])
            });
        }
    } else {
        ValidateOrderedTabletReplicationProgress(options.ReplicationProgress);

        const auto& segments = options.ReplicationProgress.Segments;
        const auto& timestampColumn = schema->GetColumnOrThrow(TimestampColumnName);
        timestampColumnIndex = schema->GetColumnIndex(timestampColumn);

        int index = segments[0].LowerKey.GetCount() > 0
            ? FromUnversionedValue<i64>(segments[0].LowerKey[0])
            : 0;

        if (index >= std::ssize(tableInfo->Tablets)) {
            THROW_ERROR_EXCEPTION("Target queue has no corresponding tablet")
                << TErrorAttribute("tablet_index", index)
                << TErrorAttribute("tablet_count", std::ssize(tableInfo->Tablets));
        }

        requests.push_back({
            .TabletIndex = index,
            .StartReplicationRowIndex = getStartReplicationRowIndex(index),
            .Progress = options.ReplicationProgress
        });
    }

    struct TPullRowsOutputBufferTag { };
    std::vector<TIntrusivePtr<TTabletPullRowsSession>> sessions;
    std::vector<TFuture<void>> futureResults;
    for (const auto& request : requests) {
        sessions.push_back(New<TTabletPullRowsSession>(
            this,
            schema,
            timestampColumnIndex,
            tableInfo->Tablets[request.TabletIndex],
            tableInfo->IsSorted(),
            options,
            request,
            Connection_->GetInvoker(),
            Logger));
        futureResults.push_back(sessions.back()->RunRequest());
    }

    WaitFor(AllSet(std::move(futureResults)))
        .ThrowOnError();

    TTimestamp maxTimestamp = MaxTimestamp;
    if (options.OrderRowsByTimestamp) {
        for (const auto& session : sessions) {
            if (session->GetReplicationProgress().Segments.size() != 1) {
                THROW_ERROR_EXCEPTION("Invalid replication progress in pull rows session")
                    << TErrorAttribute("tablet_id", session->GetTabletInfo()->TabletId)
                    << TErrorAttribute("replication_progress", session->GetReplicationProgress());
            }
            maxTimestamp = std::min(maxTimestamp, GetReplicationProgressMinTimestamp(session->GetReplicationProgress()));
        }
    }

    TPullRowsResult combinedResult;
    std::vector<TTypeErasedRow> resultRows;
    auto outputRowBuffer = New<TRowBuffer>(TPullRowsOutputBufferTag());

    bool success = false;
    for (const auto& session : sessions) {
        if (!session->GetResultOrError().IsOK()) {
            continue;
        }

        success = true;

        const auto& rows = session->GetRows(maxTimestamp, outputRowBuffer);
        resultRows.insert(resultRows.end(), rows.begin(), rows.end());

        const auto& replicationProgress = maxTimestamp == MaxTimestamp
            ? session->GetReplicationProgress()
            : LimitReplicationProgressByTimestamp(session->GetReplicationProgress(), maxTimestamp);
        combinedResult.ReplicationProgress.Segments.insert(
            combinedResult.ReplicationProgress.Segments.end(),
            replicationProgress.Segments.begin(),
            replicationProgress.Segments.end());

        if (auto endReplicationRowIndex = session->GetEndReplicationRowIndex()) {
            combinedResult.EndReplicationRowIndexes[session->GetTabletInfo()->TabletId] = *endReplicationRowIndex;
        }

        combinedResult.DataWeight += session->GetDataWeight();
        combinedResult.RowCount += session->GetRowCount();
    }

    if (!success) {
        TError error("All pull rows subrequests failed");
        for (const auto& session : sessions) {
            error.MutableInnerErrors()->push_back(session->GetResultOrError());
        }
        THROW_ERROR_EXCEPTION(error);
    }

    if (tableInfo->IsSorted() && options.OrderRowsByTimestamp) {
        std::sort(resultRows.begin(), resultRows.end(), [&] (const auto& lhs, const auto& rhs) {
            return ExtractTimestampFromPulledRow(TVersionedRow(lhs)) < ExtractTimestampFromPulledRow(TVersionedRow(rhs));
        });
    }

    CanonizeReplicationProgress(&combinedResult.ReplicationProgress);
    combinedResult.ReplicationProgress.UpperKey = options.ReplicationProgress.UpperKey;
    combinedResult.Rowset = CreateRowset(
        schema,
        MakeSharedRange(std::move(resultRows), std::move(outputRowBuffer)));
    combinedResult.Versioned = tableInfo->IsSorted();

    YT_LOG_DEBUG("Pulled rows (ReplicationProgress: %v, EndRowIndexes: %v)",
        combinedResult.ReplicationProgress,
        combinedResult.EndReplicationRowIndexes);

    return combinedResult;
}

IChannelPtr TClient::GetChaosChannelByCellId(TCellId cellId, EPeerKind peerKind)
{
    return GetNativeConnection()->GetChaosChannelByCellId(cellId, peerKind);
}

IChannelPtr TClient::GetChaosChannelByCellTag(TCellTag cellTag, EPeerKind peerKind)
{
    return GetNativeConnection()->GetChaosChannelByCellTag(cellTag, peerKind);
}

IChannelPtr TClient::GetChaosChannelByCardId(TReplicationCardId replicationCardId, EPeerKind peerKind)
{
    return GetNativeConnection()->GetChaosChannelByCardId(replicationCardId, peerKind);
}

TReplicationCardPtr TClient::GetSyncReplicationCard(const TTableMountInfoPtr& tableInfo)
{
    const auto& mountCacheConfig = Connection_->GetStaticConfig()->TableMountCache;
    auto fetchOptions = TReplicationCardFetchOptions{
        .IncludeCoordinators = true,
        .IncludeProgress = true,
        .IncludeHistory = true,
    };
    const auto& replicationCardCache = GetReplicationCardCache();
    TReplicationCardPtr replicationCard;

    for (int retryCount = 0; retryCount < mountCacheConfig->OnErrorRetryCount; ++retryCount) {
        YT_LOG_DEBUG("Synchronizing replication card (ReplicationCardId: %v, Attempt: %v)",
            tableInfo->ReplicationCardId,
            retryCount);

        auto key = TReplicationCardCacheKey{
            .CardId = tableInfo->ReplicationCardId,
            .FetchOptions = fetchOptions,
        };

        if (retryCount > 0) {
            if (replicationCard) {
                key.RefreshEra = replicationCard->Era;
                replicationCardCache->ForceRefresh(key, replicationCard);
            }

            TDelayedExecutor::WaitForDuration(mountCacheConfig->OnErrorSlackPeriod);
        }

        auto futureReplicationCard = replicationCardCache->GetReplicationCard(key);
        auto replicationCardOrError = WaitForFast(futureReplicationCard);

        if (!replicationCardOrError.IsOK()) {
            YT_LOG_DEBUG(replicationCardOrError, "Failed to get replication card from cache (ReplicationCardId: %v)",
                tableInfo->ReplicationCardId);
            continue;
        }

        replicationCard = replicationCardOrError.Value();

        if (replicationCard->CoordinatorCellIds.empty()) {
            YT_LOG_DEBUG("Replication card contains no coordinators (ReplicationCard: %v)",
                *replicationCard);
            continue;
        }

        auto coordinator = replicationCard->CoordinatorCellIds[RandomNumber<size_t>() % replicationCard->CoordinatorCellIds.size()];
        auto channel = GetChaosChannelByCellId(coordinator, EPeerKind::Leader);
        auto proxy = TCoordinatorServiceProxy(channel);
        proxy.SetDefaultTimeout(Connection_->GetConfig()->DefaultChaosNodeServiceTimeout);
        auto req = proxy.GetReplicationCardEra();

        ToProto(req->mutable_replication_card_id(), tableInfo->ReplicationCardId);

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(rspOrError, "Failed to get replication card from coordinator (ReplicationCardId: %v)",
                tableInfo->ReplicationCardId);
            continue;
        }

        auto rsp = rspOrError.Value();
        auto coordinatorEra = rsp->replication_era();

        YT_LOG_DEBUG("Got replication card era from coordinator (Era: %v)",
            coordinatorEra);

        if (replicationCard->Era == coordinatorEra) {
            return replicationCard;
        }

        YT_VERIFY(replicationCard->Era < coordinatorEra);

        YT_LOG_DEBUG("Replication card era mismatch coordinator era (ReplicationCardEra: %v, CoordinatorEra: %v)",
            replicationCard->Era,
            coordinatorEra);
    }

    THROW_ERROR_EXCEPTION("Unable to synchronize replication card")
        << TErrorAttribute("replication_card_id", tableInfo->ReplicationCardId);
}

TReplicationCardPtr TClient::DoGetReplicationCard(
    TReplicationCardId replicationCardId,
    const TGetReplicationCardOptions& options)
{
    if (!options.BypassCache) {
        const auto& replicationCardCache = GetReplicationCardCache();
        auto replicationCardFuture = replicationCardCache->GetReplicationCard({
            .CardId = replicationCardId,
            .FetchOptions = static_cast<const TReplicationCardFetchOptions&>(options)
        });
        return WaitForFast(replicationCardFuture)
            .ValueOrThrow();
    }

    auto channel = GetChaosChannelByCardId(replicationCardId, EPeerKind::LeaderOrFollower);
    auto proxy = TChaosNodeServiceProxy(std::move(channel));
    proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultChaosNodeServiceTimeout));

    auto req = proxy.GetReplicationCard();
    ToProto(req->mutable_replication_card_id(), replicationCardId);
    ToProto(req->mutable_fetch_options(), static_cast<const TReplicationCardFetchOptions&>(options));

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    auto replicationCard = New<TReplicationCard>();
    FromProto(replicationCard.Get(), rsp->replication_card());

    YT_LOG_DEBUG("Got replication card (ReplicationCardId: %v, ReplicationCard: %v)",
        replicationCardId,
        *replicationCard);

    return replicationCard;
}

void TClient::DoUpdateChaosTableReplicaProgress(
    TReplicaId replicaId,
    const TUpdateChaosTableReplicaProgressOptions& options)
{
    auto replicationCardId = ReplicationCardIdFromReplicaId(replicaId);
    auto channel = GetChaosChannelByCardId(replicationCardId);
    auto proxy = TChaosNodeServiceProxy(std::move(channel));
    proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultChaosNodeServiceTimeout));

    auto req = proxy.UpdateTableReplicaProgress();
    SetMutationId(req, options);
    ToProto(req->mutable_replication_card_id(), replicationCardId);
    ToProto(req->mutable_replica_id(), replicaId);
    ToProto(req->mutable_replication_progress(), options.Progress);
    req->set_force(options.Force);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoAlterReplicationCard(
    TReplicationCardId replicationCardId,
    const TAlterReplicationCardOptions& options)
{
    if (options.ReplicatedTableOptions && options.EnableReplicatedTableTracker) {
        THROW_ERROR_EXCEPTION(
            "Cannot alter replication card %v: only one of \"replicated_table_options\" "
            "and \"enable_replicated_table_tracker\" could be specified",
            replicationCardId);
    }

    auto channel = GetChaosChannelByCardId(replicationCardId);
    auto proxy = TChaosNodeServiceProxy(std::move(channel));
    proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultChaosNodeServiceTimeout));

    auto req = proxy.AlterReplicationCard();
    SetMutationId(req, options);
    ToProto(req->mutable_replication_card_id(), replicationCardId);

    if (options.ReplicatedTableOptions) {
        req->set_replicated_table_options(ConvertToYsonString(options.ReplicatedTableOptions).ToString());
    }
    if (options.EnableReplicatedTableTracker) {
        req->set_enable_replicated_table_tracker(*options.EnableReplicatedTableTracker);
    }
    if (options.ReplicationCardCollocationId) {
        ToProto(req->mutable_replication_card_collocation_id(), *options.ReplicationCardCollocationId);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
