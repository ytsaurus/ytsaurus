#include "client_impl.h"
#include "config.h"
#include "connection.h"
#include "transaction.h"
#include "tablet_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>
#include <yt/yt/ytlib/query_client/column_evaluator.h>
#include <yt/yt/ytlib/query_client/query_preparer.h>
#include <yt/yt/ytlib/query_client/functions.h>
#include <yt/yt/ytlib/query_client/functions_cache.h>
#include <yt/yt/ytlib/query_client/executor.h>
#include <yt/yt/ytlib/query_client/helpers.h>
#include <yt/yt/ytlib/query_client/explain.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/yt/ytlib/tablet_client/tablet_cell_bundle_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>

#include <library/cpp/int128/int128.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

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
        for (int index = 0; index < row.GetKeyCount(); ++index) {
            auto id = row.BeginKeys()[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row.BeginKeys()[index].Id = mapping[id];
        }
        for (int index = 0; index < row.GetValueCount(); ++index) {
            auto id = row.BeginValues()[index].Id;
            YT_VERIFY(id < mapping.size() && mapping[id] != -1);
            row.BeginValues()[index].Id = mapping[id];
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TQueryPreparer
    : public virtual TRefCounted
    , public IPrepareCallbacks
{
public:
    TQueryPreparer(
        NTabletClient::ITableMountCachePtr mountTableCache,
        IInvokerPtr invoker)
        : MountTableCache_(std::move(mountTableCache))
        , Invoker_(std::move(invoker))
    { }

    // IPrepareCallbacks implementation.
    TFuture<TDataSplit> GetInitialSplit(
        const TYPath& path,
        TTimestamp timestamp) override
    {
        return BIND(&TQueryPreparer::DoGetInitialSplit, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(path, timestamp);
    }

private:
    const NTabletClient::ITableMountCachePtr MountTableCache_;
    const IInvokerPtr Invoker_;

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

    TDataSplit DoGetInitialSplit(
        const TRichYPath& path,
        TTimestamp timestamp)
    {
        auto tableInfo = WaitFor(MountTableCache_->GetTableInfo(path.GetPath()))
            .ValueOrThrow();

        tableInfo->ValidateNotReplicated();

        TDataSplit result;
        SetObjectId(&result, tableInfo->TableId);
        SetTableSchema(&result, *GetTableSchema(path, tableInfo));
        SetTimestamp(&result, timestamp);
        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryPreparer)

////////////////////////////////////////////////////////////////////////////////

std::vector<TTabletInfo> TClient::DoGetTabletInfos(
    const TYPath& path,
    const std::vector<int>& tabletIndexes,
    const TGetTabletsInfoOptions& options)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    struct TSubrequest
    {
        TQueryServiceProxy::TReqGetTabletInfoPtr Request;
        std::vector<size_t> ResultIndexes;
    };

    THashMap<TCellId, TSubrequest> cellIdToSubrequest;

    for (size_t resultIndex = 0; resultIndex < tabletIndexes.size(); ++resultIndex) {
        auto tabletIndex = tabletIndexes[resultIndex];
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(tabletIndex);
        auto& subrequest = cellIdToSubrequest[tabletInfo->CellId];
        if (!subrequest.Request) {
            auto channel = GetReadCellChannelOrThrow(tabletInfo->CellId);
            TQueryServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultGetTabletInfosTimeout));
            subrequest.Request = proxy.GetTabletInfo();
        }
        ToProto(subrequest.Request->add_tablet_ids(), tabletInfo->TabletId);
        ToProto(subrequest.Request->add_cell_ids(), tabletInfo->CellId);
        subrequest.ResultIndexes.push_back(resultIndex);
    }

    std::vector<TFuture<TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncRspsOrErrors;
    std::vector<const TSubrequest*> subrequests;
    for (const auto& [cellId, subrequest] : cellIdToSubrequest) {
        subrequests.push_back(&subrequest);
        asyncRspsOrErrors.push_back(subrequest.Request->Invoke());
    }

    auto rspsOrErrors = WaitFor(AllSucceeded(asyncRspsOrErrors))
        .ValueOrThrow();

    std::vector<TTabletInfo> results(tabletIndexes.size());
    for (size_t subrequestIndex = 0; subrequestIndex < rspsOrErrors.size(); ++subrequestIndex) {
        const auto& subrequest = *subrequests[subrequestIndex];
        const auto& rsp = rspsOrErrors[subrequestIndex];
        YT_VERIFY(rsp->tablets_size() == std::ssize(subrequest.ResultIndexes));
        for (size_t resultIndexIndex = 0; resultIndexIndex < subrequest.ResultIndexes.size(); ++resultIndexIndex) {
            auto& result = results[subrequest.ResultIndexes[resultIndexIndex]];
            const auto& tabletInfo = rsp->tablets(static_cast<int>(resultIndexIndex));
            result.TotalRowCount = tabletInfo.total_row_count();
            result.TrimmedRowCount = tabletInfo.trimmed_row_count();
            result.BarrierTimestamp = tabletInfo.barrier_timestamp();
            result.LastWriteTimestamp = tabletInfo.last_write_timestamp();
            result.TableReplicaInfos = tabletInfo.replicas().empty()
                ? std::nullopt
                : std::make_optional(std::vector<TTabletInfo::TTableReplicaInfo>());

            for (const auto& protoReplicaInfo : tabletInfo.replicas()) {
                auto& currentReplica = result.TableReplicaInfos->emplace_back();
                currentReplica.ReplicaId = FromProto<TGuid>(protoReplicaInfo.replica_id());
                currentReplica.LastReplicationTimestamp = protoReplicaInfo.last_replication_timestamp();
                currentReplica.Mode = CheckedEnumCast<ETableReplicaMode>(protoReplicaInfo.mode());
                currentReplica.CurrentReplicationRowIndex = protoReplicaInfo.current_replication_row_index();
            }
        }
    }
    return results;
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
        TWireProtocolWriter writer;
        writer.WriteCommand(EWireProtocolCommand::LookupRows);
        writer.WriteMessage(req);
        writer.WriteSchemafulRowset(remappedKeys);
        return writer.Finish();
    };
}

TClient::TDecoderWithMapping TClient::GetLookupRowsDecoder() const
{
    return [] (
        const TSchemaData& schemaData,
        TWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadSchemafulRow(schemaData, true).ToTypeErasedRow();
    };
}

IUnversionedRowsetPtr TClient::DoLookupRows(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TLookupRowsOptions& options)
{
    TReplicaFallbackHandler<IUnversionedRowsetPtr> fallbackHandler = [&] (
        const TReplicaFallbackInfo& replicaFallbackInfo)
    {
        return replicaFallbackInfo.Client->LookupRows(
            replicaFallbackInfo.Path,
            nameTable,
            keys,
            options);
    };

    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoLookupRowsOnce<IUnversionedRowsetPtr, TUnversionedRow>(
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

IVersionedRowsetPtr TClient::DoVersionedLookupRows(
    const TYPath& path,
    const TNameTablePtr& nameTable,
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
        TWireProtocolWriter writer;
        writer.WriteCommand(EWireProtocolCommand::VersionedLookupRows);
        writer.WriteMessage(req);
        writer.WriteSchemafulRowset(remappedKeys);
        return writer.Finish();
    };

    TDecoderWithMapping decoder = [] (
        const TSchemaData& schemaData,
        TWireProtocolReader* reader) -> TTypeErasedRow
    {
        return reader->ReadVersionedRow(schemaData, true).ToTypeErasedRow();
    };

    TReplicaFallbackHandler<IVersionedRowsetPtr> fallbackHandler = [&] (
        const TReplicaFallbackInfo& replicaFallbackInfo)
    {
        return replicaFallbackInfo.Client->VersionedLookupRows(
            replicaFallbackInfo.Path,
            nameTable,
            keys,
            options);
    };

    std::optional<TString> retentionConfig;
    if (options.RetentionConfig) {
        retentionConfig = ConvertToYsonString(options.RetentionConfig).ToString();
    }

    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoLookupRowsOnce<IVersionedRowsetPtr, TVersionedRow>(
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

std::vector<IUnversionedRowsetPtr> TClient::DoMultiLookup(
    const std::vector<TMultiLookupSubrequest>& subrequests,
    const TMultiLookupOptions& options)
{
    std::vector<TFuture<IUnversionedRowsetPtr>> asyncRowsets;
    asyncRowsets.reserve(subrequests.size());
    for (const auto& subrequest : subrequests) {
        TLookupRowsOptions lookupRowsOptions;
        static_cast<TTabletReadOptionsBase&>(lookupRowsOptions) = options;
        static_cast<TMultiplexingBandOptions&>(lookupRowsOptions) = options;
        static_cast<TLookupRequestOptions&>(lookupRowsOptions) = std::move(subrequest.Options);

        asyncRowsets.push_back(BIND(
            [
                =,
                this_ = MakeStrong(this),
                lookupRowsOptions = std::move(lookupRowsOptions)
            ] {
                TReplicaFallbackHandler<IUnversionedRowsetPtr> fallbackHandler = [&] (
                    const TReplicaFallbackInfo& replicaFallbackInfo)
                {
                    return replicaFallbackInfo.Client->LookupRows(
                        replicaFallbackInfo.Path,
                        subrequest.NameTable,
                        subrequest.Keys,
                        lookupRowsOptions);
                };

                return CallAndRetryIfMetadataCacheIsInconsistent([&] {
                    return DoLookupRowsOnce<IUnversionedRowsetPtr, TUnversionedRow>(
                        subrequest.Path,
                        subrequest.NameTable,
                        subrequest.Keys,
                        lookupRowsOptions,
                        /* retentionConfig */ std::nullopt,
                        GetLookupRowsEncoder(),
                        GetLookupRowsDecoder(),
                        fallbackHandler);
                });
            })
            .AsyncVia(GetCurrentInvoker())
            .Run());
    }

    return WaitFor(AllSucceeded(std::move(asyncRowsets)))
        .ValueOrThrow();
}

template <class TRowset, class TRow>
TRowset TClient::DoLookupRowsOnce(
    const TYPath& path,
    const TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const TLookupRowsOptionsBase& options,
    const std::optional<TString>& retentionConfig,
    TEncoderWithMapping encoderWithMapping,
    TDecoderWithMapping decoderWithMapping,
    TReplicaFallbackHandler<TRowset> replicaFallbackHandler)
{
    if (options.EnablePartialResult && options.KeepMissingRows) {
        THROW_ERROR_EXCEPTION("Options \"enable_partial_result\" and \"keep_missing_rows\" cannot be used together");
    }

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    if (options.DetailedProfilingInfo && tableInfo->EnableDetailedProfiling) {
        options.DetailedProfilingInfo->EnableDetailedProfiling = true;
        options.DetailedProfilingInfo->TablePath = path;
    }

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto idMapping = BuildColumnIdMapping(*schema, nameTable);
    auto remappedColumnFilter = RemapColumnFilter(options.ColumnFilter, idMapping, nameTable);
    auto resultSchema = tableInfo->Schemas[ETableSchemaKind::Primary]->Filter(remappedColumnFilter, true);
    auto resultSchemaData = TWireProtocolReader::GetSchemaData(*schema, remappedColumnFilter);

    NSecurityClient::TPermissionKey permissionKey{
        .Object = FromObjectId(tableInfo->TableId),
        .User = Options_.GetAuthenticatedUser(),
        .Permission = EPermission::Read,
        .Columns = GetLookupColumns(remappedColumnFilter, *schema)
    };
    const auto& permissionCache = Connection_->GetPermissionCache();
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError();

    if (keys.Empty()) {
        return CreateRowset(resultSchema, TSharedRange<TRow>());
    }

    // NB: The server-side requires the keys to be sorted.
    std::vector<std::pair<NTableClient::TLegacyKey, int>> sortedKeys;
    sortedKeys.reserve(keys.Size());

    struct TLookupRowsInputBufferTag
    { };
    auto inputRowBuffer = New<TRowBuffer>(TLookupRowsInputBufferTag());

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

    if (tableInfo->IsReplicated()) {
        auto inSyncReplicasFuture = PickInSyncReplicas(
            tableInfo,
            options,
            sortedKeys);

        TReplicaFallbackInfo replicaFallbackInfo;
        if (auto inSyncReplicasOrError = inSyncReplicasFuture.TryGet()) {
            replicaFallbackInfo = GetReplicaFallbackInfo(
                inSyncReplicasOrError->ValueOrThrow());
        } else {
            auto inSyncReplicas = WaitFor(inSyncReplicasFuture)
                .ValueOrThrow();
            replicaFallbackInfo = GetReplicaFallbackInfo(inSyncReplicas);
        }

        return WaitFor(replicaFallbackHandler(replicaFallbackInfo))
            .ValueOrThrow();
    }

    // TODO(sandello): Use code-generated comparer here.
    std::sort(sortedKeys.begin(), sortedKeys.end());
    std::vector<size_t> keyIndexToResultIndex(keys.Size());
    size_t currentResultIndex = 0;

    struct TLookupRowsOutputBufferTag
    { };
    auto outputRowBuffer = New<TRowBuffer>(TLookupRowsOutputBufferTag());

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

            ValidateTabletMountedOrFrozen(startShard);

            auto emplaced = cellIdToBatchIndex.emplace(startShard->CellId, batchesByCells.size());
            if (emplaced.second) {
                batchesByCells.emplace_back();
            }

            TBatch batch;
            batch.TabletId = startShard->TabletId;
            batch.MountRevision = startShard->MountRevision;
            batch.OffsetInResult = currentResultIndex;

            // Take an arbitrary one; these are all the same.
            inMemoryMode = startShard->InMemoryMode;

            std::vector<TLegacyKey> rows;
            rows.reserve(endItemsIt - itemsIt);

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
            batchesByCells[emplaced.first->second].push_back(std::move(batch));
        }
    }

    using TEncoder = std::function<std::vector<TSharedRef>(const std::vector<NTableClient::TUnversionedRow>&)>;
    using TDecoder = std::function<NTableClient::TTypeErasedRow(NTableClient::TWireProtocolReader*)>;

    TEncoder boundEncoder = std::bind(encoderWithMapping, remappedColumnFilter, std::placeholders::_1);
    TDecoder boundDecoder = std::bind(decoderWithMapping, resultSchemaData, std::placeholders::_1);

    auto* codec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsRequestCodec);

    std::vector<TFuture<TQueryServiceProxy::TRspMultireadPtr>> asyncResults(batchesByCells.size());

    const auto& cellDirectory = Connection_->GetCellDirectory();
    const auto& networks = Connection_->GetNetworks();

    for (auto [cellId, cellIndex] : cellIdToBatchIndex) {
        const auto& batches = batchesByCells[cellIndex];

        auto channel = CreateTabletReadChannel(
            ChannelFactory_,
            cellDirectory->GetDescriptorOrThrow(cellId),
            options,
            networks);

        TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultLookupRowsTimeout));
        proxy.SetDefaultAcknowledgementTimeout(std::nullopt);

        auto req = proxy.Multiread();
        req->SetMultiplexingBand(options.MultiplexingBand);
        req->set_request_codec(ToProto<int>(Connection_->GetConfig()->LookupRowsRequestCodec));
        req->set_response_codec(ToProto<int>(Connection_->GetConfig()->LookupRowsResponseCodec));
        req->set_timestamp(options.Timestamp);
        req->set_enable_partial_result(options.EnablePartialResult);
        req->set_use_lookup_cache(options.UseLookupCache);

        if (inMemoryMode != EInMemoryMode::None) {
            req->Header().set_uncancelable(true);
        }
        if (retentionConfig) {
            req->set_retention_config(*retentionConfig);
        }

        for (const auto& batch : batches) {
            ToProto(req->add_cell_ids(), cellId);
            ToProto(req->add_tablet_ids(), batch.TabletId);
            req->add_mount_revisions(batch.MountRevision);
            auto requestData = codec->Compress(boundEncoder(batch.Keys));
            req->Attachments().push_back(requestData);
        }

        auto* ext = req->Header().MutableExtension(NQueryClient::NProto::TReqMultireadExt::req_multiread_ext);
        ext->set_in_memory_mode(ToProto<int>(inMemoryMode));

        asyncResults[cellIndex] = req->Invoke();
    }

    auto results = WaitFor(AllSet(std::move(asyncResults)))
        .ValueOrThrow();

    uniqueResultRows.resize(currentResultIndex, TTypeErasedRow{nullptr});

    auto* responseCodec = NCompression::GetCodec(Connection_->GetConfig()->LookupRowsResponseCodec);

    for (size_t cellIndex = 0; cellIndex < results.size(); ++cellIndex) {
        if (options.EnablePartialResult && !results[cellIndex].IsOK()) {
            continue;
        }

        const auto& batches = batchesByCells[cellIndex];
        const auto& result = results[cellIndex].ValueOrThrow();

        for (size_t batchIndex = 0; batchIndex < batches.size(); ++batchIndex) {
            const auto& attachment = result->Attachments()[batchIndex];

            if (options.EnablePartialResult && attachment.Empty()) {
                continue;
            }

            auto responseData = responseCodec->Decompress(result->Attachments()[batchIndex]);
            TWireProtocolReader reader(responseData, outputRowBuffer);

            const auto& batch = batches[batchIndex];

            for (size_t index = 0; index < batch.Keys.size(); ++index) {
                uniqueResultRows[batch.OffsetInResult + index] = boundDecoder(&reader);
            }
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
    return CreateRowset(resultSchema, std::move(rowRange));
}

TSelectRowsResult TClient::DoSelectRows(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    return CallAndRetryIfMetadataCacheIsInconsistent([&] () {
        return DoSelectRowsOnce(queryString, options);
    });
}

TSelectRowsResult TClient::DoSelectRowsOnce(
    const TString& queryString,
    const TSelectRowsOptions& options)
{
    auto parsedQuery = ParseSource(queryString, EParseMode::Query);
    auto* astQuery = &std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);
    auto optionalClusterName = PickInSyncClusterAndPatchQuery(options, astQuery);
    if (optionalClusterName) {
        auto replicaClient = GetOrCreateReplicaClient(*optionalClusterName);
        auto updatedQueryString = NAst::FormatQuery(*astQuery);
        auto asyncResult = replicaClient->SelectRows(updatedQueryString, options);
        return WaitFor(asyncResult)
            .ValueOrThrow();
    }

    auto inputRowLimit = options.InputRowLimit.value_or(Connection_->GetConfig()->DefaultInputRowLimit);
    auto outputRowLimit = options.OutputRowLimit.value_or(Connection_->GetConfig()->DefaultOutputRowLimit);

    const auto& udfRegistryPath = options.UdfRegistryPath
        ? *options.UdfRegistryPath
        : Connection_->GetConfig()->UdfRegistryPath;

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);

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

    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto queryPreparer = New<TQueryPreparer>(tableMountCache, Connection_->GetInvoker());

    auto queryExecutor = CreateQueryExecutor(
        Connection_,
        Connection_->GetInvoker(),
        Connection_->GetColumnEvaluatorCache(),
        Connection_->GetQueryEvaluator(),
        ChannelFactory_,
        FunctionImplCache_);

    auto fragment = PreparePlanFragment(
        queryPreparer.Get(),
        *parsedQuery,
        fetchFunctions,
        options.Timestamp);
    const auto& query = fragment->Query;
    const auto& dataSource = fragment->DataSource;

    for (size_t index = 0; index < query->JoinClauses.size(); ++index) {
        if (query->JoinClauses[index]->ForeignKeyPrefix == 0 && !options.AllowJoinWithoutIndex) {
            const auto& ast = std::get<NAst::TQuery>(parsedQuery->AstHead.Ast);
            THROW_ERROR_EXCEPTION("Foreign table key is not used in the join clause; "
                "the query is inefficient, consider rewriting it")
                << TErrorAttribute("source", NAst::FormatJoin(ast.Joins[index]));
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
        addTableForPermissionCheck(joinClause->ForeignObjectId, joinClause->Schema);
    }

    if (options.ExecutionPool) {
        permissionKeys.push_back(NSecurityClient::TPermissionKey{
            .Object = QueryPoolsPath + "/" + NYPath::ToYPathLiteral(*options.ExecutionPool),
            .User = Options_.GetAuthenticatedUser(),
            .Permission = EPermission::Use
        });
    }

    const auto& permissionCache = Connection_->GetPermissionCache();
    auto permissionCheckErrors = WaitFor(permissionCache->Get(permissionKeys))
        .ValueOrThrow();
    for (const auto& error : permissionCheckErrors) {
        error.ThrowOnError();
    }

    if (options.DetailedProfilingInfo) {
        const auto& path = astQuery->Table.Path;
        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
            .ValueOrThrow();
        if (tableInfo->EnableDetailedProfiling) {
            options.DetailedProfilingInfo->EnableDetailedProfiling = true;
            options.DetailedProfilingInfo->TablePath = path;
        }
    }

    TQueryOptions queryOptions;
    queryOptions.Timestamp = options.Timestamp;
    queryOptions.RangeExpansionLimit = options.RangeExpansionLimit;
    queryOptions.VerboseLogging = options.VerboseLogging;
    queryOptions.EnableCodeCache = options.EnableCodeCache;
    queryOptions.MaxSubqueries = options.MaxSubqueries;
    queryOptions.WorkloadDescriptor = options.WorkloadDescriptor;
    queryOptions.InputRowLimit = inputRowLimit;
    queryOptions.OutputRowLimit = outputRowLimit;
    queryOptions.UseMultijoin = options.UseMultijoin;
    queryOptions.AllowFullScan = options.AllowFullScan;
    queryOptions.ReadSessionId = TReadSessionId::Create();
    queryOptions.MemoryLimitPerNode = options.MemoryLimitPerNode;
    queryOptions.ExecutionPool = options.ExecutionPool;
    queryOptions.Deadline = options.Timeout.value_or(Connection_->GetConfig()->DefaultSelectRowsTimeout).ToDeadLine();
    queryOptions.SuppressAccessTracking = options.SuppressAccessTracking;

    TClientChunkReadOptions chunkReadOptions{
        .WorkloadDescriptor = queryOptions.WorkloadDescriptor,
        .ReadSessionId = queryOptions.ReadSessionId
    };

    IUnversionedRowsetWriterPtr writer;
    TFuture<IUnversionedRowsetPtr> asyncRowset;
    std::tie(writer, asyncRowset) = CreateSchemafulRowsetWriter(query->GetTableSchema());

    auto statistics = WaitFor(queryExecutor->Execute(
        query,
        externalCGInfo,
        dataSource,
        writer,
        chunkReadOptions,
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
    auto parsedQuery = ParseSource(queryString, EParseMode::Query);

    const auto& udfRegistryPath = options.UdfRegistryPath
        ? *options.UdfRegistryPath
        : GetNativeConnection()->GetConfig()->UdfRegistryPath;

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);

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

    auto queryPreparer = New<TQueryPreparer>(
        GetNativeConnection()->GetTableMountCache(),
        GetNativeConnection()->GetInvoker());

    auto fragment = PreparePlanFragment(
        queryPreparer.Get(),
        *parsedQuery,
        fetchFunctions,
        options.Timestamp);

    return BuildExplainQueryYson(GetNativeConnection(), fragment, udfRegistryPath, options);
}

IAttributeDictionaryPtr TClient::ResolveExternalTable(
    const TYPath& path,
    TTableId* tableId,
    TCellTag* externalCellTag,
    const std::vector<TString>& extraAttributeKeys)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());

    {
        auto req = TObjectYPathProxy::GetBasicAttributes(path);
        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of table %v", path);
        const auto& rsp = rspOrError.Value();
        *tableId = FromProto<TTableId>(rsp->object_id());
        *externalCellTag = rsp->external_cell_tag();
    }

    if (!IsTableType(TypeFromId(*tableId))) {
        THROW_ERROR_EXCEPTION("%v is not a table", path);
    }

    IAttributeDictionaryPtr extraAttributes;
    {
        auto req = TTableYPathProxy::Get(FromObjectId(*tableId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), extraAttributeKeys);
        auto rspOrError = WaitFor(proxy->Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting extended attributes of table %v", path);
        const auto& rsp = rspOrError.Value();
        extraAttributes = ConvertToAttributes(TYsonString(rsp->value()));
    }

    return extraAttributes;
}

template <class T>
auto TClient::CallAndRetryIfMetadataCacheIsInconsistent(T&& callback) -> decltype(callback())
{
    int retryCount = 0;
    while (true) {
        TError error;

        try {
            return callback();
        } catch (const NYT::TErrorException& ex) {
            error = ex.Error();
        }

        const auto& config = Connection_->GetConfig();
        const auto& tableMountCache = Connection_->GetTableMountCache();
        bool retry;
        TTabletInfoPtr tabletInfo;
        std::tie(retry, tabletInfo) = tableMountCache->InvalidateOnError(error, false /*forceRetry*/);

        if (retry && ++retryCount <= config->TableMountCache->OnErrorRetryCount) {
            YT_LOG_DEBUG(error, "Got error, will retry (attempt %v of %v)",
                retryCount,
                config->TableMountCache->OnErrorRetryCount);
            auto now = Now();
            auto retryTime = (tabletInfo ? tabletInfo->UpdateTime : now) +
                config->TableMountCache->OnErrorSlackPeriod;
            if (retryTime > now) {
                TDelayedExecutor::WaitForDuration(retryTime - now);
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
    auto tableAttributes = ResolveExternalTable(
        path,
        &tableId,
        &externalCellTag,
        {"path"});

    if (!IsTableType(TypeFromId(tableId))) {
        THROW_ERROR_EXCEPTION("Object %v is not a table", path);
    }

    auto nativeCellTag = CellTagFromId(tableId);

    auto transactionAttributes = CreateEphemeralAttributes();
    transactionAttributes->Set(
        "title",
        Format("%v table %v", action, path));
    auto asyncTransaction = StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        TTransactionStartOptions{
            .Attributes = std::move(transactionAttributes),
            .CoordinatorMasterCellTag = nativeCellTag,
            .ReplicateToMasterCellTags = TCellTagList{externalCellTag}
        });
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

    auto proxy = CreateReadProxy<TObjectServiceProxy>(TMasterReadOptions());
    auto req = TTableYPathProxy::Get(path + "/@schema");
    auto rspOrError = WaitFor(proxy->Execute(req));
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
            return buildPivotKeys(std::numeric_limits<i32>::min(), std::numeric_limits<i32>::max(), true);
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
    auto attributes = ResolveExternalTable(
        path,
        &tableId,
        &externalCellTag,
        {"tablet_cell_bundle", "dynamic"});

    if (TypeFromId(tableId) != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid object type: expected %v, got %v",
            EObjectType::Table, TypeFromId(tableId))
            << TErrorAttribute("path", path);
    }

    if (!attributes->Get<bool>("dynamic")) {
        THROW_ERROR_EXCEPTION("Table %v must be dynamic",
            path);
    }

    auto bundle = attributes->Get<TString>("tablet_cell_bundle");
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(bundle), EPermission::Use);

    auto req = TTableYPathProxy::ReshardAutomatic(FromObjectId(tableId));
    SetMutationId(req, options);
    req->set_keep_actions(options.KeepActions);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(externalCellTag);
    auto protoRsp = WaitFor(proxy->Execute(req))
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
    if (options.Dynamic) {
        req->set_dynamic(*options.Dynamic);
    }
    if (options.UpstreamReplicaId) {
        ToProto(req->mutable_upstream_replica_id(), *options.UpstreamReplicaId);
    }
    if (options.SchemaModification) {
        req->set_schema_modification(static_cast<int>(*options.SchemaModification));
    }

    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
    WaitFor(proxy->Execute(req))
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
    ValidateTableReplicaPermission(replicaId, EPermission::Write);

    auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
    if (options.Enabled) {
        req->set_enabled(*options.Enabled);
    }
    if (options.Mode) {
        req->set_mode(static_cast<int>(*options.Mode));
    }
    if (options.PreserveTimestamps) {
        req->set_preserve_timestamps(*options.PreserveTimestamps);
    }
    if (options.Atomicity) {
        req->set_atomicity(static_cast<int>(*options.Atomicity));
    }

    auto cellTag = CellTagFromId(replicaId);
    auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

TYsonString TClient::DoGetTablePivotKeys(
    const NYPath::TYPath& path,
    const TGetTablePivotKeysOptions& /*options*/)
{
    const auto& tableMountCache = Connection_->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    auto keySchema = tableInfo->Schemas[ETableSchemaKind::Primary]->ToKeys();

    return BuildYsonStringFluently()
        .DoListFor(tableInfo->Tablets, [&] (TFluentList fluent, const TTabletInfoPtr& tablet) {
            fluent
                .Item()
                .DoMapFor(tablet->PivotKey, [&] (TFluentMap fluent, const TUnversionedValue& value) {
                    if (value.Id <= keySchema->GetColumnCount()) {
                        fluent
                            .Item(keySchema->Columns()[value.Id].Name())
                            .Value(value);
                    }
                });
        });
}

std::vector<TTabletActionId> TClient::DoBalanceTabletCells(
    const TString& tabletCellBundle,
    const std::vector<TYPath>& movableTables,
    const TBalanceTabletCellsOptions& options)
{
    InternalValidatePermission("//sys/tablet_cell_bundles/" + ToYPathLiteral(tabletCellBundle), EPermission::Use);

    std::vector<TFuture<TTabletCellBundleYPathProxy::TRspBalanceTabletCellsPtr>> cellResponses;

    if (movableTables.empty()) {
        auto cellTags = Connection_->GetSecondaryMasterCellTags();
        cellTags.push_back(Connection_->GetPrimaryMasterCellTag());
        auto req = TTabletCellBundleYPathProxy::BalanceTabletCells("//sys/tablet_cell_bundles/" + tabletCellBundle);
        SetMutationId(req, options);
        req->set_keep_actions(options.KeepActions);
        for (const auto& cellTag : cellTags) {
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
        }
    } else {
        THashMap<TCellTag, std::vector<TTableId>> tablesByCells;

        for (const auto& path : movableTables) {
            TTableId tableId;
            TCellTag externalCellTag;
            auto attributes = ResolveExternalTable(
                path,
                &tableId,
                &externalCellTag,
                {"dynamic", "tablet_cell_bundle"});

            if (TypeFromId(tableId) != EObjectType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid object type: expected %v, got %v",
                    EObjectType::Table,
                    TypeFromId(tableId))
                    << TErrorAttribute("path", path);
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
            auto proxy = CreateWriteProxy<TObjectServiceProxy>(cellTag);
            cellResponses.emplace_back(proxy->Execute(req));
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

std::vector<TAlienCellDescriptor> TClient::DoSyncAlienCells(
    const std::vector<TAlienCellDescriptorLite>& alienCellDescriptors,
    const TSyncAlienCellOptions& options)
{
    auto channel = GetMasterChannelOrThrow(options.ReadFrom, PrimaryMasterCellTag); 
    auto proxy = TChaosMasterServiceProxy(channel);
    auto req = proxy.SyncAlienCells();
    
    ToProto(req->mutable_cell_descriptors(), alienCellDescriptors);

    auto res = WaitFor(req->Invoke())
        .ValueOrThrow();

    return FromProto<std::vector<TAlienCellDescriptor>>(res->cell_descriptors());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
