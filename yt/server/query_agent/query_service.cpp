#include "query_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/query_agent/config.h>
#include <yt/server/query_agent/helpers.h>

#include <yt/server/tablet_node/security_manager.h>
#include <yt/server/tablet_node/slot_manager.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/transaction_manager.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/schemaful_writer.h>

#include <yt/client/table_client/wire_protocol.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NQueryAgent {

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

class TQueryService
    : public TServiceBase
{
public:
    TQueryService(
        TQueryAgentConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetQueryPoolInvoker({}),
            TQueryServiceProxy::GetDescriptor(),
            QueryAgentLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetCancelable(true)
            .SetInvoker(bootstrap->GetLookupPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Multiread)
            .SetInvoker(bootstrap->GetLookupPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTabletInfo)
            .SetInvoker(bootstrap->GetLookupPoolInvoker()));
    }

private:
    const TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        LOG_DEBUG("Deserializing subfragment");

        auto query = FromProto<TConstQueryPtr>(request->query());
        context->SetRequestInfo("FragmentId: %v", query->Id);

        auto externalCGInfo = New<TExternalCGInfo>();
        FromProto(&externalCGInfo->Functions, request->external_functions());
        externalCGInfo->NodeDirectory->MergeFrom(request->node_directory());

        auto options = FromProto<TQueryOptions>(request->options());
        options.InputRowLimit = request->query().input_row_limit();
        options.OutputRowLimit = request->query().output_row_limit();

        auto dataSources = FromProto<std::vector<TDataRanges>>(request->data_sources());

        LOG_DEBUG("Deserialized subfragment (FragmentId: %v, InputRowLimit: %v, OutputRowLimit: %v, "
            "RangeExpansionLimit: %v, MaxSubqueries: %v, EnableCodeCache: %v, WorkloadDescriptor: %v, "
            "ReadSesisonId: %v, DataRangeCount: %v)",
            query->Id,
            options.InputRowLimit,
            options.OutputRowLimit,
            options.RangeExpansionLimit,
            options.MaxSubqueries,
            options.EnableCodeCache,
            options.WorkloadDescriptor,
            options.ReadSessionId,
            dataSources.size());

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = options.WorkloadDescriptor;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = options.ReadSessionId;

        ExecuteRequestWithRetries(
            Config_->MaxQueryRetries,
            Logger,
            [&] () {
                auto codecId = ECodec(request->response_codec());
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
                    options);
                auto result = WaitFor(asyncResult)
                    .ValueOrThrow();

                response->Attachments() = writer->GetCompressedBlocks();
                ToProto(response->mutable_query_statistics(), result);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Read)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto timestamp = TTimestamp(request->timestamp());
        auto requestCodecId = NCompression::ECodec(request->request_codec());
        auto responseCodecId = NCompression::ECodec(request->response_codec());

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

        const auto &slotManager = Bootstrap_->GetTabletSlotManager();

        try {
            ExecuteRequestWithRetries(
                Config_->MaxQueryRetries,
                Logger,
                [&] {
                    auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
                    slotManager->ValidateTabletAccess(
                        tabletSnapshot,
                        EPermission::Read,
                        timestamp);
                    tabletSnapshot->ValidateMountRevision(mountRevision);

                    struct TLookupRowBufferTag { };
                    TWireProtocolReader reader(requestData, New<TRowBuffer>(TLookupRowBufferTag()));
                    TWireProtocolWriter writer;

                    const auto &tabletManager = tabletSnapshot->TabletManager;
                    tabletManager->Read(
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
        auto requestCodecId = NCompression::ECodec(request->request_codec());
        auto responseCodecId = NCompression::ECodec(request->response_codec());
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

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        const auto &slotManager = Bootstrap_->GetTabletSlotManager();

        size_t batchCount = request->tablet_ids_size();
        YCHECK(batchCount == request->mount_revisions_size());
        YCHECK(batchCount == request->Attachments().size());

        auto* requestCodec = NCompression::GetCodec(requestCodecId);
        auto* responseCodec = NCompression::GetCodec(responseCodecId);

        for (size_t index = 0; index < batchCount; ++index) {
            auto tabletId = FromProto<TTabletId>(request->tablet_ids(index));
            auto mountRevision = request->mount_revisions(index);

            try {
                ExecuteRequestWithRetries(
                    Config_->MaxQueryRetries,
                    Logger,
                    [&] {
                        context->SetRequestInfo("TabletId: %v, Timestamp: %llx, RequestCodec: %v, ResponseCodec: %v, ReadSessionId: %v, RetentionConfig: %v",
                            tabletId,
                            timestamp,
                            requestCodecId,
                            responseCodecId,
                            blockReadOptions.ReadSessionId,
                            retentionConfig);

                        auto requestData = requestCodec->Decompress(request->Attachments()[index]);

                        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
                        slotManager->ValidateTabletAccess(
                            tabletSnapshot,
                            EPermission::Read,
                            timestamp);
                        tabletSnapshot->ValidateMountRevision(mountRevision);

                        struct TLookupRowBufferTag { };
                        TWireProtocolReader reader(requestData, New<TRowBuffer>(TLookupRowBufferTag()));
                        TWireProtocolWriter writer;

                        const auto &tabletManager = tabletSnapshot->TabletManager;
                        tabletManager->Read(
                            tabletSnapshot,
                            timestamp,
                            user,
                            blockReadOptions,
                            retentionConfig,
                            &reader,
                            &writer);

                        response->Attachments().push_back(responseCodec->Compress(writer.Finish()));
                    });
            } catch (const TErrorException&) {
                if (auto tabletSnapshot = slotManager->FindTabletSnapshot(tabletId)) {
                    ++tabletSnapshot->PerformanceCounters->LookupErrorCount;
                }

                throw;
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, GetTabletInfo)
    {
        auto tabletIds = FromProto<std::vector<TTabletId>>(request->tablet_ids());

        context->SetRequestInfo("TabletIds: %v", tabletIds);

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();

        for (const auto& tabletId : tabletIds) {
            const auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

            auto* protoTabletInfo = response->add_tablets();
            ToProto(protoTabletInfo->mutable_tablet_id(), tabletId);
            protoTabletInfo->set_total_row_count(tabletSnapshot->RuntimeData->TotalRowCount.load());
            protoTabletInfo->set_trimmed_row_count(tabletSnapshot->RuntimeData->TrimmedRowCount.load());

            for (const auto& replicaPair : tabletSnapshot->Replicas) {
                const auto& replicaId = replicaPair.first;
                const auto& replicaSnapshot = replicaPair.second;

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
};

IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

