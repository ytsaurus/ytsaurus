#include "query_executor.h"
#include "query_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/lib/misc/profiling_helpers.h>

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/server/node/query_agent/config.h>

#include <yt/server/node/tablet_node/security_manager.h>
#include <yt/server/node/tablet_node/slot_manager.h>
#include <yt/server/node/tablet_node/store.h>
#include <yt/server/node/tablet_node/tablet.h>
#include <yt/server/node/tablet_node/tablet_reader.h>
#include <yt/server/node/tablet_node/tablet_slot.h>
#include <yt/server/node/tablet_node/tablet_manager.h>
#include <yt/server/node/tablet_node/lookup.h>
#include <yt/server/node/tablet_node/transaction_manager.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/unversioned_writer.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/wire_protocol.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT::NQueryAgent {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NCompression;
using namespace NConcurrency;
using namespace NHydra;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = QueryAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

// TODO(ifsmirnov): YT_12491 - move this to reader config and dynamically choose
// row count based on desired streaming window data size.
static constexpr size_t MaxRowsPerRemoteDynamicStoreRead = 1024;

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error)
{
    return
        error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed) ||
        error.FindMatching(NChunkClient::EErrorCode::NoSuchChunk) ||
        error.FindMatching(NTabletClient::EErrorCode::TabletSnapshotExpired);
}

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
            if (IsRetriableError(error)) {
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

void ValidateColumnFilterContainsAllKeyColumns(
    const TColumnFilter& columnFilter,
    const TTableSchema& schema)
{
    if (columnFilter.IsUniversal()) {
        return;
    }

    for (int columnIndex = 0; columnIndex < schema.GetKeyColumnCount(); ++columnIndex) {
        if (!columnFilter.ContainsIndex(columnIndex)) {
            THROW_ERROR_EXCEPTION("Column filter does not contain key column %Qv with index %v",
                schema.Columns()[columnIndex].Name(),
                columnIndex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TServiceBase
{
public:
    TQueryService(
        TQueryAgentConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetQueryPoolInvoker({}, 1.0, {}),
            TQueryServiceProxy::GetDescriptor(),
            QueryAgentLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetCancelable(true)
            .SetInvoker(bootstrap->GetTabletLookupPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Multiread)
            .SetCancelable(true)
            .SetInvoker(bootstrap->GetTabletLookupPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfo)
            .SetInvoker(bootstrap->GetTabletLookupPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadDynamicStore)
            .SetCancelable(true)
            .SetStreamingEnabled(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
    }

private:
    const TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        TServiceProfilerGuard profilerGuard(&Profiler, "/execute");

        YT_LOG_DEBUG("Deserializing subfragment");

        auto query = FromProto<TConstQueryPtr>(request->query());
        context->SetRequestInfo("FragmentId: %v", query->Id);

        auto externalCGInfo = New<TExternalCGInfo>();
        FromProto(&externalCGInfo->Functions, request->external_functions());
        externalCGInfo->NodeDirectory->MergeFrom(request->node_directory());

        auto options = FromProto<TQueryOptions>(request->options());
        options.InputRowLimit = request->query().input_row_limit();
        options.OutputRowLimit = request->query().output_row_limit();

        auto dataSources = FromProto<std::vector<TDataRanges>>(request->data_sources());

        YT_LOG_DEBUG("Deserialized subfragment (FragmentId: %v, InputRowLimit: %v, OutputRowLimit: %v, "
            "RangeExpansionLimit: %v, MaxSubqueries: %v, EnableCodeCache: %v, WorkloadDescriptor: %v, "
            "ReadSesisonId: %v, MemoryLimitPerNode: %v, DataRangeCount: %v)",
            query->Id,
            options.InputRowLimit,
            options.OutputRowLimit,
            options.RangeExpansionLimit,
            options.MaxSubqueries,
            options.EnableCodeCache,
            options.WorkloadDescriptor,
            options.ReadSessionId,
            options.MemoryLimitPerNode,
            dataSources.size());

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = options.WorkloadDescriptor;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = options.ReadSessionId;

        ExecuteRequestWithRetries<void>(
            Config_->MaxQueryRetries,
            Logger,
            [&] {
                auto codecId = CheckedEnumCast<ECodec>(request->response_codec());
                auto writer = CreateWireProtocolRowsetWriter(
                    codecId,
                    Config_->DesiredUncompressedResponseBlockSize,
                    query->GetTableSchema(),
                    false,
                    Logger);

                const auto& executor = Bootstrap_->GetQueryExecutor();
                auto asyncResult = executor->Execute(
                    query,
                    externalCGInfo,
                    dataSources,
                    writer,
                    blockReadOptions,
                    options,
                    profilerGuard);
                auto result = WaitFor(asyncResult)
                    .ValueOrThrow();

                response->Attachments() = writer->GetCompressedBlocks();
                ToProto(response->mutable_query_statistics(), result);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Read)
    {
        TServiceProfilerGuard profilerGuard(&Profiler, "/read");

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto timestamp = TTimestamp(request->timestamp());
        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto responseCodecId = CheckedEnumCast<NCompression::ECodec>(request->response_codec());

        TClientBlockReadOptions blockReadOptions;
        // TODO(sandello): Extract this out of RPC request.
        blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive);
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = TReadSessionId::Create();

        TRetentionConfigPtr retentionConfig;
        if (request->has_retention_config()) {
            retentionConfig = ConvertTo<TRetentionConfigPtr>(TYsonString(request->retention_config()));
        }

        context->SetRequestInfo("TabletId: %v, Timestamp: %llx, RequestCodec: %v, ResponseCodec: %v, ReadSessionId: %v, RetentionConfig: %v",
            tabletId,
            timestamp,
            requestCodecId,
            responseCodecId,
            blockReadOptions.ReadSessionId,
            retentionConfig);

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto* responseCodec = NCompression::GetCodec(responseCodecId);

        auto requestData = requestCodec->Decompress(request->Attachments()[0]);

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        try {
            ExecuteRequestWithRetries<void>(
                Config_->MaxQueryRetries,
                Logger,
                [&] {
                    auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

                    if (tabletSnapshot->IsProfilingEnabled() && profilerGuard.GetProfilerTags().empty()) {
                        profilerGuard.SetProfilerTags(AddUserTag(user, tabletSnapshot->ProfilerTags));
                    }

                    slotManager->ValidateTabletAccess(tabletSnapshot, timestamp);

                    tabletSnapshot->ValidateMountRevision(mountRevision);

                    struct TLookupRowBufferTag { };
                    TWireProtocolReader reader(requestData, New<TRowBuffer>(TLookupRowBufferTag()));
                    TWireProtocolWriter writer;

                    LookupRead(
                        tabletSnapshot,
                        timestamp,
                        user,
                        blockReadOptions,
                        retentionConfig,
                        &reader,
                        &writer);

                    response->Attachments().push_back(responseCodec->Compress(writer.Finish()));
                    context->Reply();
                });
        } catch (const TErrorException&) {
            if (auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId)) {
                ++tabletSnapshot->PerformanceCounters->LookupErrorCount;
            }

            throw;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Multiread)
    {
        TServiceProfilerGuard profilerGuard(&Profiler, "/multiread");

        auto requestCodecId = CheckedEnumCast<NCompression::ECodec>(request->request_codec());
        auto responseCodecId = CheckedEnumCast<NCompression::ECodec>(request->response_codec());
        auto timestamp = TTimestamp(request->timestamp());
        // TODO(sandello): Extract this out of RPC request.
        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive);
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = TReadSessionId::Create();

        TRetentionConfigPtr retentionConfig;
        if (request->has_retention_config()) {
            retentionConfig = ConvertTo<TRetentionConfigPtr>(TYsonString(request->retention_config()));
        }

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        size_t batchCount = request->tablet_ids_size();
        YT_VERIFY(batchCount == request->mount_revisions_size());
        YT_VERIFY(batchCount == request->Attachments().size());

        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());

        context->SetRequestInfo("TabletIds: %v, Timestamp: %llx, RequestCodec: %v, ResponseCodec: %v, ReadSessionId: %v, RetentionConfig: %v",
            tabletIds,
            timestamp,
            requestCodecId,
            responseCodecId,
            blockReadOptions.ReadSessionId,
            retentionConfig);

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto* responseCodec = NCompression::GetCodec(responseCodecId);

        std::vector<TCallback<TFuture<TSharedRef>()>> batchCallbacks;
        for (size_t index = 0; index < batchCount; ++index) {
            auto tabletId = tabletIds[index];
            auto mountRevision = request->mount_revisions(index);
            auto attachment = request->Attachments()[index];

            if (auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId)) {
                if (tabletSnapshot->IsProfilingEnabled() && profilerGuard.GetProfilerTags().empty()) {
                    const auto& user = context->GetUser();
                    profilerGuard.SetProfilerTags(AddUserTag(user, tabletSnapshot->ProfilerTags));
                }
            }

            auto callback = BIND([=] () -> TSharedRef {
                try {
                    return ExecuteRequestWithRetries<TSharedRef>(
                        Config_->MaxQueryRetries,
                        Logger,
                        [&] () -> TSharedRef {
                            const auto& user = context->GetUser();
                            const auto& securityManager = Bootstrap_->GetSecurityManager();
                            TAuthenticatedUserGuard userGuard(securityManager, user);

                            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

                            slotManager->ValidateTabletAccess(tabletSnapshot, timestamp);
                            tabletSnapshot->ValidateMountRevision(mountRevision);

                            auto requestData = requestCodec->Decompress(attachment);

                            struct TLookupRowBufferTag { };
                            TWireProtocolReader reader(requestData, New<TRowBuffer>(TLookupRowBufferTag()));
                            TWireProtocolWriter writer;

                            LookupRead(
                                tabletSnapshot,
                                timestamp,
                                user,
                                blockReadOptions,
                                retentionConfig,
                                &reader,
                                &writer);

                            return responseCodec->Compress(writer.Finish());
                        });
                } catch (const TErrorException&) {
                    if (auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId)) {
                        ++tabletSnapshot->PerformanceCounters->LookupErrorCount;
                    }

                    throw;
                }
            }).AsyncVia(Bootstrap_->GetTabletLookupPoolInvoker());

            batchCallbacks.push_back(callback);
        }

        auto results = WaitFor(RunWithBoundedConcurrency(batchCallbacks, Config_->MaxSubqueries))
            .ValueOrThrow();

        for (const auto& result : results) {
            if (request->enable_partial_result() && !result.IsOK()) {
                response->Attachments().emplace_back();
                continue;
            }

            response->Attachments().push_back(result.ValueOrThrow());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, GetTabletInfo)
    {
        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());

        context->SetRequestInfo("TabletIds: %v", tabletIds);

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        for (auto tabletId : tabletIds) {
            auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

            auto* protoTabletInfo = response->add_tablets();
            ToProto(protoTabletInfo->mutable_tablet_id(), tabletId);
            // NB: Read barrier timestamp first to ensure a certain degree of consistency with TotalRowCount.
            protoTabletInfo->set_barrier_timestamp(tabletSnapshot->TabletCellRuntimeData->BarrierTimestamp.load());
            protoTabletInfo->set_total_row_count(tabletSnapshot->TabletRuntimeData->TotalRowCount.load());
            protoTabletInfo->set_trimmed_row_count(tabletSnapshot->TabletRuntimeData->TrimmedRowCount.load());
            protoTabletInfo->set_last_write_timestamp(tabletSnapshot->TabletRuntimeData->LastWriteTimestamp.load());

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
            }
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, ReadDynamicStore)
    {
        auto storeId = FromProto<TDynamicStoreId>(request->store_id());
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto readSessionId = FromProto<TReadSessionId>(request->read_session_id());

        context->SetRequestInfo("StoreId: %v, TabletId: %v, ReadSessionId: %v, Timestamp: %llx",
            storeId,
            tabletId,
            readSessionId,
            request->timestamp());

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

        if (tabletSnapshot->IsPreallocatedDynamicStoreId(storeId)) {
            YT_LOG_DEBUG("Dynamic store is not created yet, sending nothing (TabletId: %v, StoreId: %v, "
                "ReadSessionId: %v, RequestId: %v)",
                tabletId,
                storeId,
                readSessionId,
                context->GetRequestId());
            HandleInputStreamingRequest(context, BIND([] {
                return MakeFuture(TSharedRef{});
            }));
            return;
        }

        auto dynamicStore = tabletSnapshot->GetDynamicStoreOrThrow(storeId);

        TColumnFilter columnFilter;
        if (request->has_column_filter()) {
            columnFilter = TColumnFilter(FromProto<TColumnFilter::TIndexes>(request->column_filter().indexes()));
            ValidateColumnFilter(columnFilter, tabletSnapshot->PhysicalSchema.GetColumnCount());
            ValidateColumnFilterContainsAllKeyColumns(columnFilter, tabletSnapshot->PhysicalSchema);
        }

        auto bandwidthThrottler = Bootstrap_->GetTabletNodeOutThrottler(EWorkloadCategory::UserDynamicStoreRead);

        bool sorted = tabletSnapshot->PhysicalSchema.IsSorted();

        if (sorted) {
            auto lowerBound = request->has_lower_bound()
                ? FromProto<TOwningKey>(request->lower_bound())
                : MinKey();
            auto upperBound = request->has_upper_bound()
                ? FromProto<TOwningKey>(request->upper_bound())
                : MaxKey();
            TTimestamp timestamp = request->timestamp();

            // NB: Options and throttler are not used by the reader.
            auto reader = dynamicStore->AsSorted()->CreateReader(
                tabletSnapshot,
                MakeSingletonRowRange(lowerBound, upperBound),
                timestamp,
                /*produceAllVersions*/ false,
                columnFilter,
                TClientBlockReadOptions(),
                GetUnlimitedThrottler());
            WaitFor(reader->Open())
                .ThrowOnError();

            std::vector<TVersionedRow> rows;
            rows.reserve(MaxRowsPerRemoteDynamicStoreRead);

            TWireProtocolWriter writer;
            writer.WriteVersionedRowset(rows);

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

            return HandleInputStreamingRequest(context, BIND([&] {
                // NB: Dynamic store reader is non-blocking in the sense of ready event.
                // However, waiting on blocked row may occur. See YT-12492.
                reader->Read(&rows);
                if (rows.empty()) {
                    return MakeFuture(TSharedRef{});
                }

                TWireProtocolWriter writer;
                writer.WriteVersionedRowset(rows);
                auto data = writer.Finish();
                auto mergedRef = MergeRefsToRef<int>(data);

                auto throttleResult = WaitFor(bandwidthThrottler->Throttle(mergedRef.size()));
                if (!throttleResult.IsOK()) {
                    auto error = TError("Failed to throttle out bandwidth in dynamic store reader")
                        << throttleResult;
                    error.ThrowOnError();
                }

                return MakeFuture(mergedRef);
            }));
        } else {
            THROW_ERROR_EXCEPTION("Remote reader for ordered dynamic stores is not implemented");
        }
    }
};

IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

