#include "query_executor.h"
#include "query_service.h"
#include "public.h"
#include "private.h"
#include "replication_log_batch_reader.h"
#include "session_manager.h"
#include "session.h"
#include "helpers.h"
#include "multiread_request_queue_provider.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/error_manager.h>
#include <yt/yt/server/node/tablet_node/error_reporting_service_base.h>
#include <yt/yt/server/node/tablet_node/lookup.h>
#include <yt/yt/server/node/tablet_node/master_connector.h>
#include <yt/yt/server/node/tablet_node/overload_controlling_service_base.h>
#include <yt/yt/server/node/tablet_node/security_manager.h>
#include <yt/yt/server/node/tablet_node/store.h>
#include <yt/yt/server/node/tablet_node/replication_log.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/tablet_manager.h>
#include <yt/yt/server/node/tablet_node/tablet_reader.h>
#include <yt/yt/server/node/tablet_node/tablet_slot.h>
#include <yt/yt/server/node/tablet_node/tablet_snapshot_store.h>
#include <yt/yt/server/node/tablet_node/transaction_manager.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>
#include <yt/yt/ytlib/query_client/functions_cache.h>
#include <yt/yt/ytlib/query_client/tracked_memory_chunk_provider.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/api/internal_client.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NQueryAgent {

using namespace NClusterNode;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NCompression;
using namespace NConcurrency;
using namespace NHydra;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TMiscExt;
using NYT::ToProto;
using NYT::FromProto;
using NQueryClient::TDistributedSessionId;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(ifsmirnov)
static constexpr i64 MaxRowsPerRemoteDynamicStoreRead = 1024;

static const TString DefaultQLExecutionPoolName = "default";
static const TString DefaultQLExecutionTag = "default";

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ExecuteRequestWithRetries(
    int maxRetries,
    const NLogging::TLogger& logger,
    const std::function<T()>& callback)
{
    const auto& Logger = logger;
    std::vector<TError> errors;
    for (int retryIndex = 0; retryIndex < maxRetries; ++retryIndex) {
        try {
            return callback();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (IsRetriableQueryError(error)) {
                YT_LOG_INFO(error, "Request failed, retrying");
                errors.push_back(error);
                continue;
            } else {
                throw;
            }
        }
    }
    THROW_ERROR_EXCEPTION("Request failed after %v retries", maxRetries)
        << errors;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnFilterContainsAllKeyColumns(
    const TColumnFilter& columnFilter,
    const TTableSchema& schema)
{
    if (columnFilter.IsUniversal()) {
        return;
    }

    for (int columnIndex = 0; columnIndex < schema.GetKeyColumnCount(); ++columnIndex) {
        if (!columnFilter.ContainsIndex(columnIndex)) {
            THROW_ERROR_EXCEPTION("Column filter does not contain key column %v with index %v",
                schema.Columns()[columnIndex].GetDiagnosticNameString(),
                columnIndex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTypeErasedRow ReadVersionedReplicationRow(
    const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
    const IReplicationLogParserPtr& logParser,
    const TRowBufferPtr& rowBuffer,
    TUnversionedRow replicationLogRow,
    TTimestamp* timestamp)
{
    TTypeErasedRow replicationRow;
    NApi::ERowModificationType modificationType;
    i64 rowIndex;

    logParser->ParseLogRow(
        tabletSnapshot,
        replicationLogRow,
        rowBuffer,
        &replicationRow,
        &modificationType,
        &rowIndex,
        timestamp,
        /*isVersioned*/ true);

    return replicationRow;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TErrorReportingServiceBase<TOverloadControllingServiceBase<TServiceBase>>
{
public:
    TQueryService(
        TQueryAgentConfigPtr config,
        NTabletNode::IBootstrap* bootstrap)
        : TErrorReportingServiceBase(
            bootstrap,
            bootstrap,
            bootstrap->GetQueryPoolInvoker(
                DefaultQLExecutionPoolName,
                DefaultQLExecutionTag),
            TQueryServiceProxy::GetDescriptor(),
            QueryAgentLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Config_(config)
        , Bootstrap_(bootstrap)
        , FunctionImplCache_(CreateFunctionImplCache(
            config->FunctionImplCache,
            bootstrap->GetClient()))
        , Evaluator_(CreateEvaluator(Config_, QueryAgentProfiler))
        , MemoryTracker_(
            Bootstrap_
                ->GetMemoryUsageTracker()
                ->WithCategory(EMemoryCategory::Query))
        , DistributedSessionManager_(CreateDistributedSessionManager(
            bootstrap->GetQueryPoolInvoker(DefaultQLExecutionPoolName, DefaultQLExecutionTag)))
        , RejectUponThrottlerOverdraft_(Config_->RejectUponThrottlerOverdraft)
        , RejectInMemoryRequestsUponThrottlerOverdraft_(Config_->RejectUponThrottlerOverdraft)
        , MaxPullQueueResponseDataWeight_(Config_->MaxPullQueueResponseDataWeight)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true)
            .SetInvokerProvider(BIND(&TQueryService::GetExecuteInvoker, Unretained(this)))
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Multiread)
            .SetCancelable(true)
            .SetInvoker(Bootstrap_->GetTabletLookupPoolInvoker())
            .SetRequestQueueProvider(MultireadRequestQueueProvider_)
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PullRows)
            .SetCancelable(true)
            .SetInvoker(Bootstrap_->GetTabletLookupPoolInvoker())
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfo)
            .SetInvoker(Bootstrap_->GetTabletLookupPoolInvoker())
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadDynamicStore)
            .SetCancelable(true)
            .SetStreamingEnabled(true)
            .SetResponseCodec(NCompression::ECodec::Lz4)
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FetchTabletStores)
            .SetInvoker(Bootstrap_->GetTabletFetchPoolInvoker())
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FetchTableRows)
            .SetInvoker(Bootstrap_->GetTableRowFetchPoolInvoker())
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetOrderedTabletSafeTrimRowCount)
            .SetInvoker(Bootstrap_->GetTableRowFetchPoolInvoker())
            .SetHandleMethodError(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateDistributedSession)
            .SetInvoker(bootstrap->GetQueryPoolInvoker(DefaultQLExecutionPoolName, DefaultQLExecutionTag)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingDistributedSession)
            .SetInvoker(bootstrap->GetQueryPoolInvoker(DefaultQLExecutionPoolName, DefaultQLExecutionTag)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CloseDistributedSession)
            .SetInvoker(bootstrap->GetQueryPoolInvoker(DefaultQLExecutionPoolName, DefaultQLExecutionTag)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PushRowset)
            .SetCancelable(true)
            .SetInvokerProvider(BIND(&TQueryService::GetExecuteInvoker, Unretained(this))));

        Bootstrap_->GetDynamicConfigManager()->SubscribeConfigChanged(BIND(
            &TQueryService::OnDynamicConfigChanged,
            MakeWeak(this)));
        SubscribeLoadAdjusted();
    }

private:
    const TQueryAgentConfigPtr Config_;
    NTabletNode::IBootstrap* const Bootstrap_;

    const TFunctionImplCachePtr FunctionImplCache_;
    const IEvaluatorPtr Evaluator_;
    const IMemoryUsageTrackerPtr MemoryTracker_;
    const TMemoryProviderMapByTagPtr MemoryProvider_ = New<TMemoryProviderMapByTag>();
    const IRequestQueueProviderPtr MultireadRequestQueueProvider_ = CreateMultireadRequestQueueProvider();
    const IDistributedSessionManagerPtr DistributedSessionManager_;

    std::atomic<bool> RejectUponThrottlerOverdraft_;
    std::atomic<bool> RejectInMemoryRequestsUponThrottlerOverdraft_;
    std::atomic<i64> MaxPullQueueResponseDataWeight_;

    NProfiling::TCounter TabletErrorCountCounter_ = QueryAgentProfiler.Counter("/get_tablet_infos/errors/count");
    NProfiling::TCounter TabletErrorSizeCounter_ = QueryAgentProfiler.Counter("/get_tablet_infos/errors/byte_size");

    IInvokerPtr GetExecuteInvoker(const NRpc::NProto::TRequestHeader& requestHeader)
    {
        const auto& ext = requestHeader.GetExtension(NQueryClient::NProto::TReqExecuteExt::req_execute_ext);

        auto tag = ext.has_execution_tag()
            ? ext.execution_tag()
            : DefaultQLExecutionTag;

        auto poolName = ext.has_execution_pool()
            ? ext.execution_pool()
            : DefaultQLExecutionPoolName;

        return Bootstrap_->GetQueryPoolInvoker(poolName, tag);
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        const auto& requestHeaderExt = context->RequestHeader().GetExtension(NQueryClient::NProto::TReqExecuteExt::req_execute_ext);
        context->SetRequestInfo("ExecutionPool: %v",
            requestHeaderExt.execution_pool());

        TServiceProfilerGuard profilerGuard;

        auto query = FromProto<TConstQueryPtr>(request->query());
        context->SetIncrementalResponseInfo("FragmentId: %v", query->Id);

        auto externalCGInfo = New<TExternalCGInfo>();
        FromProto(&externalCGInfo->Functions, request->external_functions());
        externalCGInfo->NodeDirectory->MergeFrom(request->node_directory());

        auto queryOptions = FromProto<TQueryOptions>(request->options());
        queryOptions.InputRowLimit = request->query().input_row_limit();
        queryOptions.OutputRowLimit = request->query().output_row_limit();

        auto memoryChunkProvider = MemoryProvider_->GetProvider(
            ToString(queryOptions.ReadSessionId),
            queryOptions.MemoryLimitPerNode,
            MemoryTracker_);

        // TODO(lukyan): Use memoryChunkProvider in FromProto.
        auto dataSources = FromProto<std::vector<TDataSource>>(request->data_sources());

        YT_LOG_DEBUG("Query deserialized (FragmentId: %v, InputRowLimit: %v, OutputRowLimit: %v, "
            "RangeExpansionLimit: %v, MaxSubqueries: %v, EnableCodeCache: %v, WorkloadDescriptor: %v, "
            "ReadSesisonId: %v, MemoryLimitPerNode: %v, DataRangeCount: %v)",
            query->Id,
            queryOptions.InputRowLimit,
            queryOptions.OutputRowLimit,
            queryOptions.RangeExpansionLimit,
            queryOptions.MaxSubqueries,
            queryOptions.EnableCodeCache,
            queryOptions.WorkloadDescriptor,
            queryOptions.ReadSessionId,
            queryOptions.MemoryLimitPerNode,
            dataSources.size());

        if (RejectUponThrottlerOverdraft_.load(std::memory_order::relaxed)) {
            TClientChunkReadOptions chunkReadOptions{
                .WorkloadDescriptor = queryOptions.WorkloadDescriptor,
                .ReadSessionId = queryOptions.ReadSessionId
            };

            ThrowUponNodeThrottlerOverdraft(
                context->GetStartTime(),
                context->GetTimeout(),
                chunkReadOptions,
                Bootstrap_);
        }

        // Grab the invoker provided by GetExecuteInvoker.
        auto invoker = GetCurrentInvoker();

        ExecuteRequestWithRetries<void>(
            Config_->MaxQueryRetries,
            Logger,
            [&] {
                auto codecId = CheckedEnumCast<ECodec>(request->response_codec());
                // TODO(lukyan): Use memoryChunkProvider in WireProtocolWriter.
                auto writer = CreateWireProtocolRowsetWriter(
                    codecId,
                    Config_->DesiredUncompressedResponseBlockSize,
                    query->GetTableSchema(),
                    false,
                    Logger);

                auto statistics = ExecuteSubquery(
                    Config_,
                    FunctionImplCache_,
                    Bootstrap_,
                    Evaluator_,
                    query,
                    externalCGInfo,
                    dataSources,
                    writer,
                    memoryChunkProvider,
                    invoker,
                    queryOptions,
                    profilerGuard);

                statistics.MemoryUsage = memoryChunkProvider->GetMaxAllocated();

                YT_LOG_DEBUG("Query evaluation finished (TotalMemoryUsage: %v)",
                    statistics.MemoryUsage);

                response->Attachments() = writer->GetCompressedBlocks();
                ToProto(response->mutable_query_statistics(), statistics);
                context->Reply();
            });
    }

    bool ShouldRejectUponNodeThrottlerOverdraft(EInMemoryMode mode) const
    {
        if (mode == EInMemoryMode::None) {
            return RejectUponThrottlerOverdraft_.load(std::memory_order::relaxed);
        }

        return RejectInMemoryRequestsUponThrottlerOverdraft_.load(std::memory_order::relaxed);
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Multiread)
    {
        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto responseCodecId = CheckedEnumCast<NCompression::ECodec>(request->response_codec());
        auto timestamp = FromProto<TTimestamp>(request->timestamp());
        auto retentionTimestamp = FromProto<TTimestamp>(request->retention_timestamp());

        // TODO(sandello): Extract this out of RPC request.
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive),
            .ReadSessionId = TReadSessionId::Create(),
            .MultiplexingBand = EMultiplexingBand::Interactive,
        };

        TRetentionConfigPtr retentionConfig;
        if (request->has_retention_config()) {
            retentionConfig = ConvertTo<TRetentionConfigPtr>(TYsonStringBuf(request->retention_config()));
        }

        int tabletCount = request->tablet_ids_size();
        if (tabletCount != request->mount_revisions_size()) {
            THROW_ERROR_EXCEPTION("Wrong number of mount revisions: expected %v, got %v",
                tabletCount,
                request->mount_revisions_size());
        }
        if (tabletCount != request->cell_ids_size()) {
            THROW_ERROR_EXCEPTION("Wrong number of cell ids: expected %v, got %v",
                tabletCount,
                request->cell_ids_size());
        }
        if (tabletCount != std::ssize(request->Attachments())) {
            THROW_ERROR_EXCEPTION("Wrong number of attachments: expected %v, got %v",
                tabletCount,
                request->mount_revisions_size());
        }

        const auto& requestHeaderExt = context->RequestHeader().GetExtension(NQueryClient::NProto::TReqMultireadExt::req_multiread_ext);
        auto inMemoryMode = FromProto<EInMemoryMode>(requestHeaderExt.in_memory_mode());

        context->SetRequestInfo("TabletIds: %v, Timestamp: %v, RetentionTimestamp: %v, RequestCodec: %v, ResponseCodec: %v, "
            "ReadSessionId: %v, InMemoryMode: %v, RetentionConfig: %v",
            MakeFormattableView(request->tablet_ids(), [] (auto* builder, const auto& protoTabletId) {
                FormatValue(builder, FromProto<TTabletId>(protoTabletId), TStringBuf());
            }),
            timestamp,
            retentionTimestamp,
            requestCodecId,
            responseCodecId,
            chunkReadOptions.ReadSessionId,
            inMemoryMode,
            retentionConfig);

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto* responseCodec = NCompression::GetCodec(responseCodecId);

        std::optional<bool> useLookupCache;
        if (request->has_use_lookup_cache()) {
            useLookupCache = request->use_lookup_cache();
        }

        if (ShouldRejectUponNodeThrottlerOverdraft(inMemoryMode)) {
            ThrowUponNodeThrottlerOverdraft(
                context->GetStartTime(),
                context->GetTimeout(),
                chunkReadOptions,
                Bootstrap_);
        }

        auto lookupSession = CreateLookupSession(
            inMemoryMode,
            tabletCount,
            responseCodec,
            Config_->MaxQueryRetries,
            Config_->MaxSubqueries,
            TReadTimestampRange{
                .Timestamp = timestamp,
                .RetentionTimestamp = retentionTimestamp
            },
            useLookupCache,
            std::move(chunkReadOptions),
            std::move(retentionConfig),
            request->enable_partial_result(),
            Bootstrap_->GetTabletSnapshotStore(),
            GetProfilingUser(NRpc::GetCurrentAuthenticationIdentity()),
            Bootstrap_->GetTabletLookupPoolInvoker());

        for (int index = 0; index < tabletCount; ++index) {
            auto tabletId = FromProto<TTabletId>(request->tablet_ids(index));
            auto cellId = FromProto<TCellId>(request->cell_ids(index));
            auto mountRevision = request->mount_revisions(index);

            // TODO(akozhikhov): Consider compressing/decompressing all requests' data at once.
            auto requestData = requestCodec->Decompress(request->Attachments()[index]);
            lookupSession->AddTabletRequest(
                tabletId,
                cellId,
                mountRevision,
                std::move(requestData));
        }

        auto future = lookupSession->Run();

        if (auto maybeResult = future.TryGetUnique()) {
            auto results = std::move(maybeResult->ValueOrThrow());
            YT_VERIFY(std::ssize(results) == tabletCount);
            response->Attachments() = std::move(results);
            context->Reply();
        } else {
            context->ReplyFrom(future.ApplyUnique(BIND(
                [
                    =,
                    context = context
                ] (std::vector<TSharedRef>&& results) {
                    YT_VERIFY(std::ssize(results) == tabletCount);
                    response->Attachments() = std::move(results);
                })));
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, PullRows)
    {
        auto upstreamReplicaId = FromProto<TReplicaId>(request->upstream_replica_id());
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        auto mountRevision = request->mount_revision();
        auto responseCodecId = CheckedEnumCast<NCompression::ECodec>(request->response_codec());
        auto progress = FromProto<NChaosClient::TReplicationProgress>(request->start_replication_progress());
        auto startReplicationRowIndex = request->has_start_replication_row_index()
            ? std::make_optional(request->start_replication_row_index())
            : std::nullopt;
        auto upperTimestamp = request->upper_timestamp();

        // TODO(savrus): Extract this out of RPC request.
        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletReplication),
            .ReadSessionId = TReadSessionId::Create(),
        };

        TRowBatchReadOptions rowBatchReadOptions{
            .MaxRowsPerRead = request->max_rows_per_read(),
        };

        context->SetRequestInfo("TabletId: %v, StartReplicationRowIndex: %v, Progress: %v, UpperTimestamp: %v, ResponseCodec: %v, ReadSessionId: %v)",
            tabletId,
            startReplicationRowIndex,
            progress,
            upperTimestamp,
            responseCodecId,
            chunkReadOptions.ReadSessionId);

        auto* responseCodec = NCompression::GetCodec(responseCodecId);

        TServiceProfilerGuard profilerGuard;
        auto identity = NRpc::GetCurrentAuthenticationIdentity();
        auto currentProfilingUser = GetProfilingUser(identity);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        ExecuteRequestWithRetries<void>(
            Config_->MaxQueryRetries,
            Logger,
            [&] {
                auto tabletSnapshot = snapshotStore->GetTabletSnapshotOrThrow(tabletId, cellId, mountRevision);

                SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

                if (tabletSnapshot->UpstreamReplicaId != upstreamReplicaId) {
                    THROW_ERROR_EXCEPTION(
                        NTabletClient::EErrorCode::UpstreamReplicaMismatch,
                        "Mismatched upstream replica: expected %v, got %v",
                        tabletSnapshot->UpstreamReplicaId,
                        upstreamReplicaId);
                }

                // NB: We rely on replication progress here instead of barrier timestamp since barrier timestamp could correspond to another era,
                // e.g. consider sync -> async change when replication progress is still in sync period and barrier is already in async.
                auto replicationProgress = tabletSnapshot->TabletRuntimeData->ReplicationProgress.Acquire();

                if (upperTimestamp && !IsReplicationProgressGreaterOrEqual(*replicationProgress, upperTimestamp)) {
                    upperTimestamp = NullTimestamp;
                }

                YT_LOG_DEBUG("Trying to get replication log batch for pull rows (ReplicationProgress: %v, UpperTimestamp: %v)",
                    static_cast<NChaosClient::TReplicationProgress>(*replicationProgress),
                    upperTimestamp);

                auto serviceCounters = tabletSnapshot->TableProfiler->GetQueryServiceCounters(currentProfilingUser);
                profilerGuard.Start(serviceCounters->PullRows);

                snapshotStore->ValidateTabletAccess(tabletSnapshot, AsyncLastCommittedTimestamp);
                tabletSnapshot->ValidateMountRevision(mountRevision);

                auto logParser = CreateReplicationLogParser(
                    tabletSnapshot->TableSchema,
                    tabletSnapshot->Settings.MountConfig,
                    EWorkloadCategory::SystemTabletReplication,
                    Logger);
                auto writer = CreateWireProtocolWriter();

                auto rowBuffer = New<TRowBuffer>();
                i64 readRowCount = 0;
                i64 responseRowCount = 0;
                i64 responseDataWeight = 0;
                auto maxTimestamp = MinTimestamp;
                bool readAllRows = true;
                std::optional<i64> endReplicationRowIndex;

                auto trimmedRowCount = tabletSnapshot->TabletRuntimeData->TrimmedRowCount.load();
                auto totalRowCount = tabletSnapshot->TabletRuntimeData->TotalRowCount.load();

                YT_LOG_DEBUG("Reading replication log (TrimmedRowCount: %v, TotalRowCount: %v)",
                    trimmedRowCount,
                    totalRowCount);

                auto startRowIndex = logParser->ComputeStartRowIndex(
                    tabletSnapshot,
                    GetReplicationProgressMinTimestamp(progress),
                    chunkReadOptions,
                    startReplicationRowIndex);

                if (startRowIndex) {
                    auto currentRowIndex = *startRowIndex;

                    ReadReplicationBatch(
                        tabletSnapshot,
                        chunkReadOptions,
                        rowBatchReadOptions,
                        progress,
                        logParser,
                        rowBuffer,
                        &currentRowIndex,
                        upperTimestamp,
                        writer.get(),
                        &readRowCount,
                        &responseRowCount,
                        &responseDataWeight,
                        &maxTimestamp,
                        &readAllRows);

                    endReplicationRowIndex = currentRowIndex;
                } else if (startReplicationRowIndex) {
                    endReplicationRowIndex = startReplicationRowIndex;
                }

                YT_LOG_DEBUG("Read replication batch (LastTimestamp: %v, ReadAllRows: %v, UpperTimestamp: %v, ProgressMinTimestamp: %v)",
                    maxTimestamp,
                    readAllRows,
                    upperTimestamp,
                    GetReplicationProgressMinTimestamp(*replicationProgress));

                if (readAllRows) {
                    if (upperTimestamp) {
                        maxTimestamp = upperTimestamp;
                    }

                    maxTimestamp = std::max(
                        maxTimestamp,
                        GetReplicationProgressMinTimestamp(*replicationProgress));
                }

                auto endProgress = AdvanceReplicationProgress(progress, maxTimestamp);

                // TODO(savrus, akozhikhov): Use Finally here to track failed requests.
                auto counters = tabletSnapshot->TableProfiler->GetPullRowsCounters(GetCurrentProfilingUser());
                counters->DataWeight.Increment(responseDataWeight);
                counters->RowCount.Increment(responseRowCount);
                counters->WastedRowCount.Increment(readRowCount - responseRowCount);
                counters->ChunkReaderStatisticsCounters.Increment(
                    chunkReadOptions.ChunkReaderStatistics,
                    /*failed*/ false);

                response->set_row_count(responseRowCount);
                response->set_data_weight(responseDataWeight);
                if (endReplicationRowIndex) {
                    response->set_end_replication_row_index(*endReplicationRowIndex);
                }
                ToProto(response->mutable_end_replication_progress(), endProgress);
                response->Attachments().push_back(responseCodec->Compress(writer->Finish()));

                context->SetResponseInfo("RowCount: %v, DataWeight: %v, ProcessedRowCount: %v, EndRowIndex: %v, Progress: %v",
                    responseRowCount,
                    responseDataWeight,
                    readRowCount,
                    endReplicationRowIndex,
                    endProgress);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, GetTabletInfo)
    {
        context->SetRequestInfo("TabletIds: %v, RequestErrors: %v",
            MakeFormattableView(request->tablet_ids(), [] (auto* builder, const auto& protoTabletId) {
                FormatValue(builder, FromProto<TTabletId>(protoTabletId), TStringBuf());
            }),
            request->request_errors());

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        int tabletCount = request->tablet_ids_size();
        if (tabletCount != request->cell_ids_size()) {
            THROW_ERROR_EXCEPTION("Wrong number of cell ids: expected %v, got %v",
                tabletCount,
                request->cell_ids_size());
        }

        for (int index = 0; index < tabletCount; ++index) {
            auto tabletId = FromProto<TTabletId>(request->tablet_ids(index));
            auto cellId = FromProto<TCellId>(request->cell_ids(index));

            auto tabletSnapshot = snapshotStore->GetLatestTabletSnapshotOrThrow(tabletId, cellId);

            SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

            auto* protoTabletInfo = response->add_tablets();
            ToProto(protoTabletInfo->mutable_tablet_id(), tabletId);
            // NB: Read barrier timestamp first to ensure a certain degree of consistency with TotalRowCount.
            protoTabletInfo->set_barrier_timestamp(tabletSnapshot->TabletCellRuntimeData->BarrierTimestamp.load());
            protoTabletInfo->set_total_row_count(tabletSnapshot->TabletRuntimeData->TotalRowCount.load());
            protoTabletInfo->set_trimmed_row_count(tabletSnapshot->TabletRuntimeData->TrimmedRowCount.load());
            protoTabletInfo->set_delayed_lockless_row_count(tabletSnapshot->TabletRuntimeData->DelayedLocklessRowCount.load());
            protoTabletInfo->set_last_write_timestamp(tabletSnapshot->TabletRuntimeData->LastWriteTimestamp.load());

            if (request->request_errors()) {
                tabletSnapshot->TabletRuntimeData->Errors.ForEachError([&] (const TError& error) {
                    if (!error.IsOK()) {
                        auto* protoError = protoTabletInfo->add_tablet_errors();
                        ToProto(protoError, error);
                        TabletErrorCountCounter_.Increment(1);
                        TabletErrorSizeCounter_.Increment(protoError->ByteSize());
                    }
                });
            }

            for (const auto& [replicaId, replicaSnapshot] : tabletSnapshot->Replicas) {
                auto lastReplicationTimestamp = replicaSnapshot->RuntimeData->LastReplicationTimestamp.load();
                if (lastReplicationTimestamp == NullTimestamp) {
                    lastReplicationTimestamp = replicaSnapshot->RuntimeData->CurrentReplicationTimestamp.load();
                }

                auto* protoReplicaInfo = protoTabletInfo->add_replicas();
                ToProto(protoReplicaInfo->mutable_replica_id(), replicaId);
                protoReplicaInfo->set_last_replication_timestamp(lastReplicationTimestamp);
                protoReplicaInfo->set_mode(static_cast<int>(replicaSnapshot->RuntimeData->Mode.load()));
                protoReplicaInfo->set_current_replication_row_index(replicaSnapshot->RuntimeData->CurrentReplicationRowIndex.load());
                protoReplicaInfo->set_committed_replication_row_index(replicaSnapshot->RuntimeData->CommittedReplicationRowIndex.load());
                protoReplicaInfo->set_status(static_cast<int>(replicaSnapshot->RuntimeData->Status.load()));

                if (request->request_errors()) {
                    if (auto error = replicaSnapshot->RuntimeData->Error.Load(); !error.IsOK()) {
                        auto* protoError = protoReplicaInfo->mutable_replication_error();
                        ToProto(protoError, error);
                        TabletErrorCountCounter_.Increment(1);
                        TabletErrorSizeCounter_.Increment(protoError->ByteSize());
                    }
                }
            }
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, ReadDynamicStore)
    {
        auto storeId = FromProto<TDynamicStoreId>(request->store_id());
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto cellId = FromProto<TCellId>(request->cell_id());
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());
        auto timestamp = request->timestamp();

        context->SetRequestInfo("StoreId: %v, TabletId: %v, CellId: %v, ReadSessionId: %v, Timestamp: %v",
            storeId,
            tabletId,
            cellId,
            readSessionId,
            timestamp);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->GetLatestTabletSnapshotOrThrow(tabletId, cellId);

        SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

        if (tabletSnapshot->IsPreallocatedDynamicStoreId(storeId)) {
            YT_LOG_DEBUG("Dynamic store is not created yet, sending nothing (TabletId: %v, StoreId: %v, "
                "ReadSessionId: %v, RequestId: %v)",
                tabletId,
                storeId,
                readSessionId,
                context->GetRequestId());
            HandleInputStreamingRequest(
                context,
                [] { return TSharedRef(); });
            return;
        }

        auto profilingCounters = tabletSnapshot->TableProfiler->GetRemoteDynamicStoreReadCounters(GetCurrentProfilingUser());

        TWallTimer wallTimer;
        i64 sessionRowCount = 0;
        i64 sessionDataWeight = 0;

        auto dynamicStore = tabletSnapshot->GetDynamicStoreOrThrow(storeId);

        bool sorted = tabletSnapshot->PhysicalSchema->IsSorted();

        TColumnFilter columnFilter;
        if (request->has_column_filter()) {
            columnFilter = TColumnFilter(FromProto<TColumnFilter::TIndexes>(request->column_filter().indexes()));
            // Two extra columns are tablet_index and row_index.
            ValidateColumnFilter(columnFilter, tabletSnapshot->PhysicalSchema->GetColumnCount() + (sorted ? 0 : 2));
            ValidateColumnFilterContainsAllKeyColumns(columnFilter, *tabletSnapshot->PhysicalSchema);
        }

        auto bandwidthThrottler = Bootstrap_->GetOutThrottler(EWorkloadCategory::UserDynamicStoreRead);

        if (sorted) {
            auto lowerBound = request->has_lower_bound()
                ? FromProto<TLegacyOwningKey>(request->lower_bound())
                : MinKey();
            auto upperBound = request->has_upper_bound()
                ? FromProto<TLegacyOwningKey>(request->upper_bound())
                : MaxKey();

            // NB: Options and throttler are not used by the reader.
            auto reader = dynamicStore->AsSorted()->CreateReader(
                tabletSnapshot,
                MakeSingletonRowRange(lowerBound, upperBound),
                timestamp,
                /*produceAllVersions*/ false,
                columnFilter,
                /*chunkReadOptions*/ {},
                /*workloadCategory*/ {});
            WaitFor(reader->Open())
                .ThrowOnError();

            TRowBatchReadOptions options{
                .MaxRowsPerRead = request->has_max_rows_per_read()
                    ? request->max_rows_per_read()
                    : MaxRowsPerRemoteDynamicStoreRead
            };

            YT_LOG_DEBUG("Started serving remote dynamic store read request "
                "(TabletId: %v, StoreId: %v, Timestamp: %v, ReadSessionId: %v, "
                "LowerBound: %v, UpperBound: %v, ColumnFilter: %v, RequestId: %v)",
                tabletId,
                storeId,
                timestamp,
                readSessionId,
                lowerBound,
                upperBound,
                columnFilter,
                context->GetRequestId());

            HandleInputStreamingRequest(context, [&] {
                TFiberWallTimer timer;
                i64 rowCount = 0;
                i64 dataWeight = 0;
                auto finallyGuard = Finally([&] {
                    profilingCounters->RowCount.Increment(rowCount);
                    profilingCounters->DataWeight.Increment(dataWeight);
                    profilingCounters->CpuTime.Add(timer.GetElapsedTime());

                    sessionRowCount += rowCount;
                    sessionDataWeight += dataWeight;
                });

                // NB: Dynamic store reader is non-blocking in the sense of ready event.
                // However, waiting on blocked row may occur. See YT-12492.
                auto batch = ReadRowBatch(reader, options);
                if (!batch) {
                    return TSharedRef{};
                }
                rowCount += batch->GetRowCount();

                if (request->has_failure_probability() && RandomNumber<double>() < request->failure_probability()) {
                    THROW_ERROR_EXCEPTION("Request failed for the sake of testing");
                }

                auto writer = CreateWireProtocolWriter();
                writer->WriteVersionedRowset(batch->MaterializeRows());
                auto data = writer->Finish();

                struct TReadDynamicStoreTag { };
                auto mergedRef = MergeRefsToRef<TReadDynamicStoreTag>(data);
                dataWeight += mergedRef.size();

                auto throttleResult = WaitFor(bandwidthThrottler->Throttle(mergedRef.size()));
                THROW_ERROR_EXCEPTION_IF_FAILED(throttleResult, "Failed to throttle out bandwidth in dynamic store reader");

                return mergedRef;
            });
        } else {
            i64 startRowIndex = request->has_start_row_index()
                ? request->start_row_index()
                : 0;
            i64 endRowIndex = request->has_end_row_index()
                ? request->end_row_index()
                : std::numeric_limits<i64>::max();

            // NB: Options and throttler are not used by the reader.
            auto reader = dynamicStore->AsOrdered()->CreateReader(
                tabletSnapshot,
                /*tabletIndex*/ -1, // fake
                startRowIndex,
                endRowIndex,
                timestamp,
                columnFilter,
                /*chunkReadOptions*/ {},
                /*workloadCategory*/ {});

            YT_LOG_DEBUG("Started serving remote dynamic store read request "
                "(TabletId: %v, StoreId: %v, ReadSessionId: %v, "
                "StartRowIndex: %v, EndRowIndex: %v, ColumnFilter: %v, RequestId: %v)",
                tabletId,
                storeId,
                readSessionId,
                startRowIndex,
                endRowIndex,
                columnFilter,
                context->GetRequestId());

            bool sendOffset = true;

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = request->has_max_rows_per_read()
                    ? request->max_rows_per_read()
                    : MaxRowsPerRemoteDynamicStoreRead
            };

            HandleInputStreamingRequest(context, [&] {
                TFiberWallTimer timer;
                i64 rowCount = 0;
                i64 dataWeight = 0;
                auto finallyGuard = Finally([&] {
                    profilingCounters->RowCount.Increment(rowCount);
                    profilingCounters->DataWeight.Increment(dataWeight);
                    profilingCounters->CpuTime.Add(timer.GetElapsedTime());

                    sessionRowCount += rowCount;
                    sessionDataWeight += dataWeight;
                });

                auto batch = ReadRowBatch(reader, readOptions);
                if (!batch) {
                    return TSharedRef{};
                }
                rowCount += batch->GetRowCount();

                if (request->has_failure_probability() && RandomNumber<double>() < request->failure_probability()) {
                    THROW_ERROR_EXCEPTION("Request failed for the sake of testing");
                }

                auto writer = CreateWireProtocolWriter();

                if (sendOffset) {
                    sendOffset = false;

                    i64 offset = std::max(
                        dynamicStore->AsOrdered()->GetStartingRowIndex(),
                        startRowIndex);
                    writer->WriteInt64(offset);
                }

                writer->WriteUnversionedRowset(batch->MaterializeRows());
                auto data = writer->Finish();

                struct TReadDynamicStoreTag { };
                auto mergedRef = MergeRefsToRef<TReadDynamicStoreTag>(data);
                dataWeight += mergedRef.size();

                auto throttleResult = WaitFor(bandwidthThrottler->Throttle(mergedRef.size()));
                THROW_ERROR_EXCEPTION_IF_FAILED(throttleResult, "Failed to throttle out bandwidth in dynamic store reader");

                return mergedRef;
            });
        }

        profilingCounters->SessionRowCount.Record(sessionRowCount);
        profilingCounters->SessionDataWeight.Record(sessionDataWeight);
        profilingCounters->SessionWallTime.Record(wallTimer.GetElapsedTime());
    }

    void BuildChunkSpec(
        const IChunkStorePtr& chunk,
        TLegacyReadLimit lowerLimit,
        TLegacyReadLimit upperLimit,
        bool fetchAllMetaExtensions,
        const THashSet<int>& extensionTags,
        NChunkClient::NProto::TChunkSpec* chunkSpec)
    {
        const auto& chunkMeta = chunk->GetChunkMeta();
        const auto& miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());

        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetChunkId());

        // Adjust read ranges.
        if (chunk->IsSorted()) {
            auto sortedStore = chunk->AsSorted();

            if (sortedStore->HasNontrivialReadRange()) {
                // Adjust ranges for chunk views.
                lowerLimit.MergeLowerLegacyKey(sortedStore->GetMinKey());
                lowerLimit.MergeUpperLegacyKey(sortedStore->GetUpperBoundKey());
            } else {
                // Drop redundant ranges for chunks.
                if (lowerLimit.HasLegacyKey() && lowerLimit.GetLegacyKey() <= sortedStore->GetMinKey()) {
                    lowerLimit.SetLegacyKey({});
                }
                if (upperLimit.HasLegacyKey() && upperLimit.GetLegacyKey() >= sortedStore->GetUpperBoundKey()) {
                    upperLimit.SetLegacyKey({});
                }
            }
        }

        if (!lowerLimit.IsTrivial()) {
            ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
        }
        if (!upperLimit.IsTrivial()) {
            ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
        }

        auto localNodeId = Bootstrap_->GetNodeId();
        auto replicas = chunk->GetReplicas(localNodeId);
        ToProto(chunkSpec->mutable_replicas(), replicas);
        ToProto(chunkSpec->mutable_legacy_replicas(), TChunkReplicaWithMedium::ToChunkReplicas(replicas));

        chunkSpec->set_erasure_codec(miscExt.erasure_codec());
        chunkSpec->set_striped_erasure(miscExt.striped_erasure());

        chunkSpec->set_row_count_override(miscExt.row_count());
        chunkSpec->set_data_weight_override(miscExt.data_weight());

        *chunkSpec->mutable_chunk_meta() = chunkMeta;
        if (!fetchAllMetaExtensions) {
            FilterProtoExtensions(
                chunkSpec->mutable_chunk_meta()->mutable_extensions(),
                chunkMeta.extensions(),
                extensionTags);
        }

        if (auto overrideTimestamp = chunk->GetOverrideTimestamp()) {
            chunkSpec->set_override_timestamp(overrideTimestamp);
        }
    }

    void BuildDynamicStoreSpec(
        const IDynamicStorePtr& dynamicStore,
        TTabletId tabletId,
        const TLegacyReadLimit& lowerLimit,
        const TLegacyReadLimit& upperLimit,
        NChunkClient::NProto::TChunkSpec* chunkSpec)
    {
        ToProto(chunkSpec->mutable_chunk_id(), dynamicStore->GetId());
        ToProto(chunkSpec->mutable_tablet_id(), tabletId);

        chunkSpec->set_row_count_override(dynamicStore->GetRowCount());
        // For dynamic stores it is more or less the same.
        chunkSpec->set_data_weight_override(dynamicStore->GetUncompressedDataSize());

        auto localNodeId = Bootstrap_->GetNodeId();
        TChunkReplicaWithMedium replica(localNodeId, GenericChunkReplicaIndex, GenericMediumIndex);
        chunkSpec->add_legacy_replicas(ToProto<ui32>(replica.ToChunkReplica()));
        chunkSpec->add_replicas(ToProto<ui64>(replica));

        if (!lowerLimit.IsTrivial()) {
            ToProto(chunkSpec->mutable_lower_limit(), lowerLimit);
        }
        if (!upperLimit.IsTrivial()) {
            ToProto(chunkSpec->mutable_upper_limit(), upperLimit);
        }
    }

    std::vector<TSharedRef> GatherSamples(
        const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
        const TLegacyOwningKey& lowerBound,
        const TLegacyOwningKey& upperBound,
        i64 dataSizeBetweenSamples)
    {
        std::vector<TLegacyKey> keys;
        i64 cumulativeSize = 0;
        i64 nextSampleExpectedPosition = dataSizeBetweenSamples;

        auto tryEmitSample = [&] (const TLegacyKey& key, i64 span) {
            if (cumulativeSize >= nextSampleExpectedPosition) {
                keys.push_back(key);
                nextSampleExpectedPosition += dataSizeBetweenSamples;
            } else {
                i64 thisSamplePosition = cumulativeSize;
                i64 nextSamplePosition = cumulativeSize + span;
                if (nextSamplePosition > dataSizeBetweenSamples &&
                    (nextSamplePosition - nextSampleExpectedPosition) >
                        (nextSampleExpectedPosition - thisSamplePosition))
                {
                    keys.push_back(key);
                    nextSampleExpectedPosition += dataSizeBetweenSamples;
                }
            }
            cumulativeSize += span;
        };

        for (const auto& partition : tabletSnapshot->PartitionList) {
            if (partition->PivotKey >= upperBound) {
                break;
            }
            if (partition->NextPivotKey <= lowerBound) {
                continue;
            }

            const auto& samples = partition->SampleKeys->Keys;

            i64 partitionDataSize = 0;
            for (const auto& store : partition->Stores) {
                partitionDataSize += store->GetCompressedDataSize();
            }
            i64 span = partitionDataSize / (samples.size() + 1);

            if (partition->PivotKey >= lowerBound && partition->PivotKey < upperBound) {
                tryEmitSample(partition->PivotKey, span);
            }

            auto firstIt = std::lower_bound(samples.begin(), samples.end(), lowerBound);
            auto lastIt = std::lower_bound(samples.begin(), samples.end(), upperBound);
            while (firstIt < lastIt) {
                tryEmitSample(*firstIt++, span);
            }
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteUnversionedRowset(keys);
        return writer->Finish();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, FetchTabletStores)
    {
        context->SetRequestInfo("Subrequests: %v",
            MakeFormattableView(request->subrequests(), [] (auto* builder, const auto& subrequest) {
                builder->AppendFormat("{TabletId: %v, TableIndex: %v}",
                    FromProto<TTabletId>(subrequest.tablet_id()),
                    subrequest.table_index());
            }));

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        auto extensionTags = FromProto<THashSet<int>>(request->extension_tags());

        for (const auto& subrequest : request->subrequests()) {
            auto* subresponse = response->add_subresponses();

            auto tabletId = FromProto<TTabletId>(subrequest.tablet_id());
            auto cellId = FromProto<TCellId>(subrequest.cell_id());
            auto tableIndex = subrequest.table_index();

            try {
                NTabletNode::TTabletSnapshotPtr tabletSnapshot;
                try {
                    tabletSnapshot = subrequest.has_mount_revision()
                        ? snapshotStore->GetTabletSnapshotOrThrow(tabletId, cellId, subrequest.mount_revision())
                        : snapshotStore->GetLatestTabletSnapshotOrThrow(tabletId, cellId);
                    snapshotStore->ValidateTabletAccess(tabletSnapshot, SyncLastCommittedTimestamp);
                    snapshotStore->ValidateBundleNotBanned(tabletSnapshot);
                } catch (const std::exception& ex) {
                    subresponse->set_tablet_missing(true);
                    ToProto(subresponse->mutable_error(), TError(ex));
                    continue;
                }

                SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

                if (!tabletSnapshot->PhysicalSchema->IsSorted()) {
                    THROW_ERROR_EXCEPTION("Fetching tablet stores for ordered tablets is not implemented");
                }

                auto validateReadLimit = [] (const TLegacyReadLimit& readLimit) {
                    if (readLimit.HasOffset()) {
                        THROW_ERROR_EXCEPTION("Cannot specify offset limit for fetching tablet stores");
                    }
                    if (readLimit.HasRowIndex()) {
                        THROW_ERROR_EXCEPTION("Cannot specify row index limit for fetching tablet stores");
                    }
                    if (readLimit.HasTabletIndex()) {
                        THROW_ERROR_EXCEPTION("Cannot specify tablet index limit for fetching tablet stores");
                    }
                    if (readLimit.HasChunkIndex()) {
                        THROW_ERROR_EXCEPTION("Cannot specify chunk index limit for fetching tablet stores");
                    }
                };

                for (int rangeIndex = 0; rangeIndex < subrequest.ranges_size(); ++rangeIndex) {
                    const auto& protoRange = subrequest.ranges(rangeIndex);
                    auto range = FromProto<TLegacyReadRange>(protoRange);
                    validateReadLimit(range.LowerLimit());
                    validateReadLimit(range.UpperLimit());

                    if (subrequest.fetch_samples()) {
                        response->Attachments().emplace_back();
                    }

                    const auto& rangeLowerBound = range.LowerLimit().HasLegacyKey()
                        ? range.LowerLimit().GetLegacyKey()
                        : MinKey();
                    const auto& rangeUpperBound = range.UpperLimit().HasLegacyKey()
                        ? range.UpperLimit().GetLegacyKey()
                        : MaxKey();

                    const auto& lowerBound = ChooseMaxKey(rangeLowerBound, tabletSnapshot->PivotKey);
                    const auto& upperBound = ChooseMinKey(rangeUpperBound, tabletSnapshot->NextPivotKey);

                    if (lowerBound >= upperBound) {
                        continue;
                    }

                    TLegacyReadLimit inducedLowerBound;
                    TLegacyReadLimit inducedUpperBound;
                    if (lowerBound != MinKey()) {
                        inducedLowerBound.SetLegacyKey(lowerBound);
                    }
                    if (upperBound != MaxKey()) {
                        inducedUpperBound.SetLegacyKey(upperBound);
                    }

                    auto addStore = [&] (const IStorePtr& store) {
                        switch (store->GetType()) {
                            case EStoreType::SortedChunk: {
                                auto sortedStore = store->AsSorted();
                                if (sortedStore->GetMinKey() >= upperBound || sortedStore->GetUpperBoundKey() <= lowerBound) {
                                    return;
                                }

                                BuildChunkSpec(
                                    store->AsChunk(),
                                    inducedLowerBound,
                                    inducedUpperBound,
                                    request->fetch_all_meta_extensions(),
                                    extensionTags,
                                    subresponse->add_stores());

                                break;
                            }

                            case EStoreType::SortedDynamic:
                                if (tabletSnapshot->Settings.MountConfig->EnableDynamicStoreRead &&
                                    !request->omit_dynamic_stores())
                                {
                                    BuildDynamicStoreSpec(
                                        store->AsDynamic(),
                                        tabletId,
                                        inducedLowerBound,
                                        inducedUpperBound,
                                        subresponse->add_stores());
                                } else {
                                    return;
                                }

                                break;

                            default:
                                THROW_ERROR_EXCEPTION("Unexpected store type %Qlv",
                                    store->GetType());
                        }

                        auto* spec = subresponse->mutable_stores(subresponse->stores_size() - 1);
                        spec->set_range_index(subrequest.range_indices(rangeIndex));
                        spec->set_table_index(tableIndex);
                    };

                    for (const auto& store : tabletSnapshot->Eden->Stores) {
                        addStore(store);
                    }

                    {
                        const auto& partitions = tabletSnapshot->PartitionList;

                        auto firstIt = std::lower_bound(
                            partitions.begin(),
                            partitions.end(),
                            lowerBound,
                            [&] (const TPartitionSnapshotPtr& lhs, TLegacyKey rhs) {
                                return lhs->NextPivotKey <= rhs;
                            });
                        auto lastIt = std::lower_bound(
                            partitions.begin(),
                            partitions.end(),
                            upperBound,
                            [&] (const TPartitionSnapshotPtr& lhs, TLegacyKey rhs) {
                                return lhs->PivotKey < rhs;
                            });

                        for (auto it = firstIt; it != lastIt; ++it) {
                            for (const auto& store : (*it)->Stores) {
                                addStore(store);
                            }
                        }
                    }

                    if (subrequest.fetch_samples()) {
                        auto samples = GatherSamples(
                            tabletSnapshot,
                            lowerBound,
                            upperBound,
                            subrequest.data_size_between_samples());
                        struct TFetchTabletStoresTag {};
                        auto mergedRef = MergeRefsToRef<TFetchTabletStoresTag>(std::move(samples));
                        response->Attachments().back() = std::move(mergedRef);
                    }
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error fetching tablet %v stores",
                    tabletId)
                    << TErrorAttribute("tablet_id", tabletId)
                    << ex;
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, FetchTableRows)
    {
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto cellId = FromProto<TCellId>(request->cell_id());
        auto tabletSnapshot = request->has_mount_revision()
            ? snapshotStore->GetTabletSnapshotOrThrow(tabletId, cellId, request->mount_revision())
            : snapshotStore->GetLatestTabletSnapshotOrThrow(tabletId, cellId);

        SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

        snapshotStore->ValidateTabletAccess(tabletSnapshot, SyncLastCommittedTimestamp);
        snapshotStore->ValidateBundleNotBanned(tabletSnapshot);

        if (tabletSnapshot->PhysicalSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Fetching rows for sorted tablets is not implemented");
        }

        if (!request->has_tablet_index()) {
            THROW_ERROR_EXCEPTION("Missing obligatory \"tablet_index\" parameter");
        }

        if (!request->has_row_index()) {
            THROW_ERROR_EXCEPTION("Missing obligatory \"row_index\" parameter");
        }

        if (!request->has_max_row_count()) {
            THROW_ERROR_EXCEPTION("Missing obligatory \"max_row_count\" parameter");
        }

        if (!request->has_max_data_weight()) {
            THROW_ERROR_EXCEPTION("Missing obligatory \"max_data_weight\" parameter");
        }

        context->SetRequestInfo(
            "TabletId: %v, CellId: %v, TabletIndex: %v, RowIndex: %v, MaxRowCount: %v, MaxDataWeight: %v",
            tabletId,
            cellId,
            request->tablet_index(),
            request->row_index(),
            request->max_row_count(),
            request->max_data_weight());

        auto trimmedRowCount = tabletSnapshot->TabletRuntimeData->TrimmedRowCount.load();
        YT_LOG_DEBUG(
            "Loading current trimmed row count from tablet runtime data (TabletId: %v, TrimmedRowCount: %v)",
            tabletId,
            trimmedRowCount);

        auto rowIndex = request->row_index();
        if (trimmedRowCount > rowIndex) {
            YT_LOG_DEBUG(
                "Some of the desired rows are trimmed; reading from first untrimmed row (RowIndex: %v, TrimmedRowCount: %v)",
                rowIndex,
                trimmedRowCount);
            rowIndex = trimmedRowCount;
        }

        const auto& orderedStores = tabletSnapshot->OrderedStores;

        IOrderedStorePtr desiredStore;

        YT_LOG_DEBUG(
            "Searching for appropriate ordered store to fetch rows from (TabletId: %v, StoreCount: %v, RowIndex: %v)",
            tabletId,
            orderedStores.size(),
            rowIndex);

        if (!orderedStores.empty()) {
            // We want to find the first store containing rows with row indices >= rowIndex.
            // Ex:
            //    0      1      2       3
            // [3....][11...][23....][30...]
            // For rowIndex = 3-10 we want to read from store 0, for 11-22 from store 1, etc.
            // For rowIndex < 3 we want to read from store 0.
            auto desiredStoreIt = std::upper_bound(
                orderedStores.begin(),
                orderedStores.end(),
                rowIndex,
                [] (i64 rowIndex, const IOrderedStorePtr& store) {
                    return store->GetStartingRowIndex() > rowIndex;
                });

            desiredStore = *std::ranges::prev(desiredStoreIt, /*n*/ 1, /*bound*/ orderedStores.begin());
        }

        TFetchRowsFromOrderedStoreResult fetchRowsResult;
        auto writer = CreateWireProtocolWriter();

        if (desiredStore) {
            YT_LOG_DEBUG(
                "Found store to read from (StoreId: %v, StartingRowIndex: %v, RowCount: %v)",
                desiredStore->GetId(),
                desiredStore->GetStartingRowIndex(),
                desiredStore->GetRowCount());


            TClientChunkReadOptions chunkReadOptions;
            if (request->options().has_workload_descriptor()) {
                chunkReadOptions.WorkloadDescriptor =
                    FromProto<TWorkloadDescriptor>(request->options().workload_descriptor());
            }
            chunkReadOptions.ReadSessionId = TReadSessionId::Create();

            fetchRowsResult = FetchRowsFromOrderedStore(
                tabletSnapshot,
                desiredStore,
                request->tablet_index(),
                rowIndex,
                request->max_row_count(),
                request->max_data_weight(),
                chunkReadOptions,
                writer.get());
        }

        response->Attachments() = writer->Finish();
        context->SetResponseInfo(
            "RowCount: %v, DataWeight: %v",
            fetchRowsResult.RowCount,
            fetchRowsResult.DataWeight);
        context->Reply();
    }

    struct TFetchRowsFromOrderedStoreResult
    {
        i64 RowCount = 0;
        i64 DataWeight = 0;
    };

    TFetchRowsFromOrderedStoreResult FetchRowsFromOrderedStore(
        const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
        const IOrderedStorePtr& store,
        int tabletIndex,
        i64 rowIndex,
        i64 maxRowCount,
        i64 maxDataWeight,
        const TClientChunkReadOptions& chunkReadOptions,
        IWireProtocolWriter* writer)
    {
        auto tabletId = tabletSnapshot->TabletId;

        YT_LOG_DEBUG(
            "Fetching rows from ordered store (TabletId: %v, Store: %v, StartingRowIndex: %v, RowIndex: %v, "
            "MaxRowCount: %v, MaxDataWeight: %v)",
            tabletId,
            store->GetId(),
            store->GetStartingRowIndex(),
            rowIndex,
            maxRowCount,
            maxDataWeight);

        auto reader = store->CreateReader(
            tabletSnapshot,
            tabletIndex,
            /*lowerRowIndex*/ rowIndex,
            /*upperRowIndex*/ std::numeric_limits<i64>::max(),
            AsyncLastCommittedTimestamp,
            TColumnFilter::MakeUniversal(),
            chunkReadOptions,
            chunkReadOptions.WorkloadDescriptor.Category);

        TRowBatchReadOptions readOptions;
        readOptions.MaxRowsPerRead = std::min(readOptions.MaxRowsPerRead, maxRowCount);
        readOptions.MaxDataWeightPerRead = std::min(readOptions.MaxDataWeightPerRead, maxDataWeight);

        i64 readRows = 0;
        i64 readDataWeight = 0;
        while (auto batch = reader->Read(readOptions)) {
            if (batch->IsEmpty()) {
                YT_LOG_DEBUG(
                    "Waiting for rows from ordered store (TabletId: %v, RowIndex: %v)",
                    tabletId,
                    rowIndex + readRows);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            auto rows = batch->MaterializeRows();

            readRows += std::ssize(rows);
            maxRowCount -= std::ssize(rows);

            i64 currentReadDataWeight = GetDataWeight(rows);
            readDataWeight += currentReadDataWeight;
            maxDataWeight -= currentReadDataWeight;

            writer->WriteUnversionedRowset(rows);

            if (maxRowCount <= 0 ||
                maxDataWeight <= 0 ||
                readDataWeight >= MaxPullQueueResponseDataWeight_.load(std::memory_order::relaxed))
            {
                break;
            }

            readOptions.MaxRowsPerRead = std::min(readOptions.MaxRowsPerRead, maxRowCount);
            readOptions.MaxDataWeightPerRead = std::min(readOptions.MaxDataWeightPerRead, maxDataWeight);
        }

        YT_LOG_DEBUG(
            "Fetched rows from ordered store (TabletId: %v, RowCount: %v, DataWeight: %v)",
            tabletId,
            readRows,
            readDataWeight);

        return {
            .RowCount = readRows,
            .DataWeight = readDataWeight,
        };
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, GetOrderedTabletSafeTrimRowCount)
    {
        context->SetRequestInfo("Subrequests: %v",
            request->subrequests_size());

        std::vector<TFuture<i64>> asyncSubrequests;
        asyncSubrequests.reserve(request->subrequests_size());

        for (const auto& subrequest : request->subrequests()) {
            asyncSubrequests.push_back(BIND(
                &TQueryService::GetOrderedTabletSafeTrimRowCountImpl,
                MakeStrong(this),
                FromProto<TTabletId>(subrequest.tablet_id()),
                FromProto<TCellId>(subrequest.cell_id()),
                YT_PROTO_OPTIONAL(subrequest, mount_revision),
                subrequest.timestamp())
                .AsyncVia(GetCurrentInvoker())
                .Run());
        }

        auto subresponseOrErrors = WaitFor(AllSet(std::move(asyncSubrequests)))
            .ValueOrThrow();

        for (const auto& subresponseOrError : subresponseOrErrors) {
            auto* protoSubresponse = response->add_subresponses();

            if (subresponseOrError.IsOK()) {
                protoSubresponse->set_safe_trim_row_count(subresponseOrError.Value());
            } else {
                ToProto(protoSubresponse->mutable_error(), subresponseOrError);
            }
        }

        context->Reply();
    }

    i64 GetOrderedTabletSafeTrimRowCountImpl(
        TTabletId tabletId,
        TCellId cellId,
        std::optional<TRevision> mountRevision,
        TTimestamp timestamp)
    {
        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();

        auto tabletSnapshot = mountRevision
            ? snapshotStore->GetTabletSnapshotOrThrow(tabletId, cellId, *mountRevision)
            : snapshotStore->GetLatestTabletSnapshotOrThrow(tabletId, cellId);

        SetErrorManagerContextFromTabletSnapshot(tabletSnapshot);

        snapshotStore->ValidateTabletAccess(tabletSnapshot, SyncLastCommittedTimestamp);
        snapshotStore->ValidateBundleNotBanned(tabletSnapshot);

        if (tabletSnapshot->PhysicalSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Finding stores for sorted tablets is not implemented");
        }

        const auto& orderedStores = tabletSnapshot->OrderedStores;

        // In practice, this should rarely happen, since there is almost always at least one active store.
        if (orderedStores.empty()) {
            return tabletSnapshot->TotalRowCount;
        }

        struct TStoreSnapshot
        {
            TStoreId Id;
            i64 StartRowIndex = 0;
            i64 FinishRowIndex = 0;
            TTimestamp MinTimestamp = NullTimestamp;
            TTimestamp MaxTimestamp = NullTimestamp;
        };
        std::vector<TStoreSnapshot> storeSnapshots;
        storeSnapshots.reserve(orderedStores.size());
        for (const auto& store : orderedStores) {
            // NB: Row count must be older than timestamps, so that we don't return row indexes past the saved timestamps.
            auto rowCount = store->GetRowCount();
            storeSnapshots.push_back({
                .Id = store->GetId(),
                // NB: StartingRowIndex shouldn't change in flight.
                .StartRowIndex = store->GetStartingRowIndex(),
                .FinishRowIndex = store->GetStartingRowIndex() + rowCount,
                .MinTimestamp = store->GetMinTimestamp(),
                .MaxTimestamp = store->GetMaxTimestamp(),
            });
            // NB: FinishRowIndex, MinTimestamp and MaxTimestamp might be inconsistent with each other.
        }

        const auto& lastStore = storeSnapshots.back();

        // We want to find the first store containing timestamps >= T.
        //
        // For this we look for the first store S, such that T <= S->GetMaxTimestamp().
        // Ex:
        //    0         1        2         3
        // [3....10][11...20][23....27][30...37] (numbers are timestamps of rows)
        // For T <= 10 we want to return information about store 0.
        // For 10 < T <= 20 we want to return information about store 1.
        // For 20 < T <= 27 we want to return information about store 2.
        // For 27 < T <= 37 we want to return information about store 3.
        // For T > 37 we want to return indexes [38, +inf) and no store information.
        //
        // Since timestamps are not guaranteed to be monotonous, we have to use a linear search.
        auto desiredStoreIt = std::find_if(
            storeSnapshots.begin(),
            storeSnapshots.end(),
            [&timestamp] (const TStoreSnapshot& store) {
                return store.MaxTimestamp >= timestamp;
            });

        // Empty stores do not satisfy the predicate used in the std::find_if call above, since their
        // max timestamp is -inf.
        // The list of ordered stores is produced from a mapping of the form [startingRowIndex -> store],
        // so only the last store can potentially be empty. This is perfectly fine for us.

        return desiredStoreIt != storeSnapshots.end()
            ? desiredStoreIt->StartRowIndex
            : lastStore.FinishRowIndex;
    }

    class TTabletBatchFetcher
        : public IReplicationLogBatchFetcher
    {
    public:
        TTabletBatchFetcher(
            TLegacyOwningKey lower,
            TLegacyOwningKey upper,
            const TColumnFilter& columnFilter,
            NTabletNode::TTabletSnapshotPtr tabletSnapshot,
            const TClientChunkReadOptions& chunkReaderOptions,
            const TRowBatchReadOptions& rowBatchReadOptions,
            const NTabletClient::TTabletId& tabletId,
            const NLogging::TLogger& logger)
            : TabletId_(tabletId)
            , RowBatchReadOptions_(rowBatchReadOptions)
            , Reader_(CreateSchemafulRangeTabletReader(
                std::move(tabletSnapshot),
                columnFilter,
                std::move(lower),
                std::move(upper),
                /*timestampRange*/ {},
                chunkReaderOptions,
                /*tabletThrottlerKind*/ std::nullopt,
                EWorkloadCategory::SystemTabletReplication))
            , Logger(logger)
        { }

        IUnversionedRowBatchPtr ReadNextRowBatch(i64 currentRowIndex) override
        {
            IUnversionedRowBatchPtr batch;
            while (true) {
                batch = Reader_->Read(RowBatchReadOptions_);
                if (!batch || !batch->IsEmpty()) {
                    break;
                }

                YT_LOG_DEBUG("Waiting for replicated rows from tablet reader (TabletId: %v, StartRowIndex: %v)",
                    TabletId_,
                    currentRowIndex);

                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            }

            return batch;
        }

    private:
        const NTabletClient::TTabletId& TabletId_;
        const TRowBatchReadOptions& RowBatchReadOptions_;
        const ISchemafulUnversionedReaderPtr Reader_;
        const NLogging::TLogger Logger;
    };

    class TTabletRowBatchReader
        : public TReplicationLogBatchReaderBase
    {
    public:
        TTabletRowBatchReader(
            NTabletNode::TTabletSnapshotPtr tabletSnapshot,
            TClientChunkReadOptions chunkReaderOptions,
            TRowBatchReadOptions rowBatchReadOptions,
            TReplicationProgress progress,
            IWireProtocolWriter* writer,
            NLogging::TLogger logger)
            : TReplicationLogBatchReaderBase(
                tabletSnapshot->Settings.MountConfig,
                tabletSnapshot->TabletId,
                std::move(logger))
            , TablerSnapshotPtr_(std::move(tabletSnapshot))
            , ChunkReadOptions_(std::move(chunkReaderOptions))
            , RowBatchReadOptions_(std::move(rowBatchReadOptions))
            , Progress_(std::move(progress))
            , Writer_(writer)
        { }

    protected:
        const NTabletNode::TTabletSnapshotPtr TablerSnapshotPtr_;
        const TClientChunkReadOptions ChunkReadOptions_;
        const TRowBatchReadOptions RowBatchReadOptions_;
        const TReplicationProgress Progress_;
        IWireProtocolWriter* Writer_;

        std::unique_ptr<IReplicationLogBatchFetcher> MakeBatchFetcher(
            TLegacyOwningKey lower,
            TLegacyOwningKey upper,
            const TColumnFilter& columnFilter) const override
        {
            return std::make_unique<TTabletBatchFetcher>(
                std::move(lower),
                std::move(upper),
                columnFilter,
                TablerSnapshotPtr_,
                ChunkReadOptions_,
                RowBatchReadOptions_,
                TabletId_,
                Logger);
        }
    };

    class TOrderedRowBatchReader
        : public TTabletRowBatchReader
    {
    public:
        TOrderedRowBatchReader(
            const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
            const TClientChunkReadOptions& chunkReaderOptions,
            const TRowBatchReadOptions& rowBatchReadOptions,
            const TReplicationProgress& progress,
            const IReplicationLogParserPtr& logParser,
            IWireProtocolWriter* writer,
            const NLogging::TLogger& logger)
            : TTabletRowBatchReader(
                tabletSnapshot,
                chunkReaderOptions,
                rowBatchReadOptions,
                progress,
                writer,
                logger)
        {
            ValidateOrderedTabletReplicationProgress(progress);

            if (!logParser->GetTimestampColumnId()) {
                THROW_ERROR_EXCEPTION("Invalid table schema: %Qlv column is absent",
                    TimestampColumnName);
            }

            TimestampColumnIndex_ = *logParser->GetTimestampColumnId() + 1;
            TabletIndex_ = progress.Segments[0].LowerKey.GetCount() > 0
                ? FromUnversionedValue<i64>(progress.Segments[0].LowerKey[0])
                : 0;
        }

    protected:
        TColumnFilter CreateColumnFilter() const override
        {
            // Without a filter first two columns are (tablet index, row index). Add tablet index column to row.
            TColumnFilter::TIndexes columnFilterIndexes{0};
            for (int id = 0; id < TablerSnapshotPtr_->TableSchema->GetColumnCount(); ++id) {
                columnFilterIndexes.push_back(id + 2);
            }
            return TColumnFilter(std::move(columnFilterIndexes));
        }

        TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const override
        {
            return MakeRowBound(currentRowIndex, TabletIndex_);
        }

        bool ToTypeErasedRow(
            const TUnversionedRow& row,
            TTypeErasedRow* replicationRow,
            TTimestamp* timestamp,
            i64* rowDataWeight) const override
        {
            auto rowTimestamp = row[TimestampColumnIndex_].Data.Uint64;

            // Check that row has greater timestamp than progress.
            if (rowTimestamp <= Progress_.Segments[0].Timestamp) {
                return false;
            }

            *timestamp = rowTimestamp;
            *replicationRow = row.ToTypeErasedRow();
            *rowDataWeight = GetDataWeight(TUnversionedRow(*replicationRow));
            return true;
        }

        void WriteTypeErasedRow(TTypeErasedRow row) override
        {
            Writer_->WriteSchemafulRow(TUnversionedRow(std::move(row)));
        }

    private:
        int TimestampColumnIndex_ = 0;
        int TabletIndex_ = 0;
    };

    class TSortedRowBatchReader
        : public TTabletRowBatchReader
    {
    public:
        TSortedRowBatchReader(
            const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
            const TClientChunkReadOptions& chunkReaderOptions,
            const TRowBatchReadOptions& rowBatchReadOptions,
            const TReplicationProgress& progress,
            const TRowBufferPtr& rowBuffer,
            const IReplicationLogParserPtr& logParser,
            IWireProtocolWriter* writer,
            const NLogging::TLogger& logger)
            : TTabletRowBatchReader(
                tabletSnapshot,
                chunkReaderOptions,
                rowBatchReadOptions,
                progress,
                writer,
                logger)
            , LogParser_(logParser)
            , RowBuffer_(rowBuffer)
        { }

        TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const override
        {
            return MakeRowBound(currentRowIndex);
        }

        bool ToTypeErasedRow(
            const TUnversionedRow& row,
            TTypeErasedRow* replicationRow,
            TTimestamp* timestamp,
            i64* rowDataWeight) const override
        {
            *replicationRow = ReadVersionedReplicationRow(
                TablerSnapshotPtr_,
                LogParser_,
                RowBuffer_,
                row,
                timestamp);

            auto versionedRow = TVersionedRow(*replicationRow);
            *rowDataWeight = GetDataWeight(versionedRow);

            // Check that row fits into replication progress key range and has greater timestamp than progress.
            auto progressTimestamp = FindReplicationProgressTimestampForKey(Progress_, versionedRow.Keys());
            return progressTimestamp && *progressTimestamp < *timestamp;
        }

        void WriteTypeErasedRow(TTypeErasedRow row) override
        {
            Writer_->WriteVersionedRow(TVersionedRow(std::move((row))));
        }

    private:
        const IReplicationLogParserPtr LogParser_;
        const TRowBufferPtr RowBuffer_;
    };

    void ReadReplicationBatch(
        const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReadOptions,
        const TRowBatchReadOptions& rowBatchReadOptions,
        const TReplicationProgress& progress,
        const IReplicationLogParserPtr& logParser,
        const TRowBufferPtr& rowBuffer,
        i64* currentRowIndex,
        TTimestamp upperTimestamp,
        IWireProtocolWriter* writer,
        i64* totalRowCount,
        i64* batchRowCount,
        i64* batchDataWeight,
        TTimestamp* maxTimestamp,
        bool* readAllRows)
    {
        if (tabletSnapshot->TableSchema->IsSorted()) {
            TSortedRowBatchReader(
                tabletSnapshot,
                chunkReadOptions,
                rowBatchReadOptions,
                progress,
                rowBuffer,
                logParser,
                writer,
                Logger)
                .ReadReplicationBatch(
                    currentRowIndex,
                    upperTimestamp,
                    totalRowCount,
                    batchRowCount,
                    batchDataWeight,
                    maxTimestamp,
                    readAllRows);
        } else {
            TOrderedRowBatchReader(
                tabletSnapshot,
                chunkReadOptions,
                rowBatchReadOptions,
                progress,
                logParser,
                writer,
                Logger)
                .ReadReplicationBatch(
                    currentRowIndex,
                    upperTimestamp,
                    totalRowCount,
                    batchRowCount,
                    batchDataWeight,
                    maxTimestamp,
                    readAllRows);
        }
    }


    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        RejectUponThrottlerOverdraft_.store(
            newConfig->QueryAgent->RejectUponThrottlerOverdraft.value_or(Config_->RejectUponThrottlerOverdraft));
        RejectInMemoryRequestsUponThrottlerOverdraft_.store(
            newConfig->QueryAgent->RejectInMemoryRequestsUponThrottlerOverdraft);
        MaxPullQueueResponseDataWeight_.store(
            newConfig->QueryAgent->MaxPullQueueResponseDataWeight.value_or(Config_->MaxPullQueueResponseDataWeight));
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, CreateDistributedSession)
    {
        auto sessionId = FromProto<TDistributedSessionId>(request->session_id());
        auto retentionTime = FromProto<TDuration>(request->retention_time());
        auto codecId = CheckedEnumCast<ECodec>(request->codec());

        context->SetRequestInfo("DistributedSessionId: %v, CodecId: %v",
            sessionId,
            codecId);

        DistributedSessionManager_->GetDistributedSessionOrCreate(sessionId, retentionTime, codecId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, PingDistributedSession)
    {
        auto sessionId = FromProto<TDistributedSessionId>(request->session_id());
        auto session = DistributedSessionManager_->GetDistributedSessionOrThrow(sessionId);

        context->SetRequestInfo("DistributedSessionId: %v",
            sessionId);

        session->RenewLease();

        auto propagated = FromProto<std::vector<TString>>(request->nodes_with_propagated_session());
        session->ErasePropagationAddresses(propagated);
        ToProto(response->mutable_nodes_to_propagate_session_onto(), session->GetPropagationAddresses());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, CloseDistributedSession)
    {
        auto sessionId = FromProto<TDistributedSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v",
            sessionId);

        if (DistributedSessionManager_->CloseDistributedSession(sessionId)) {
            YT_LOG_DEBUG("Distributed query session closed remotely (SessionId: %v)", sessionId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, PushRowset)
    {
        auto sessionId = FromProto<TDistributedSessionId>(request->session_id());
        auto rowsetId = FromProto<TRowsetId>(request->rowset_id());
        auto schema = FromProto<TTableSchemaPtr>(request->schema());

        context->SetRequestInfo("SessionId: %v, RowsetId: %v",
            sessionId,
            rowsetId);

        auto session = DistributedSessionManager_->GetDistributedSessionOrThrow(sessionId);
        session->InsertOrThrow(
            CreateWireProtocolRowsetReader(
                request->Attachments(),
                session->GetCodecId(),
                schema,
                false,
                Logger),
            rowsetId);

        context->Reply();
    }
};

IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NTabletNode::IBootstrap* bootstrap)
{
    return New<TQueryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
