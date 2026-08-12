#include "shuffle_service.h"

#include "private.h"
#include "shuffle_manager.h"

#include <yt/yt/ytlib/shuffle_client/shuffle_service_proxy.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/api/shuffle_client.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NPushBasedShuffleClient;
using namespace NRpc;
using namespace NShuffleClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NApi::NNative::IClientPtr;

////////////////////////////////////////////////////////////////////////////////

class TShuffleService
    : public TServiceBase
{
public:
    TShuffleService(
        IInvokerPtr invoker,
        IClientPtr client,
        std::string localServerAddress)
        : TServiceBase(
            invoker,
            TShuffleServiceProxy::GetDescriptor(),
            ShuffleServiceLogger())
        , LocalServerAddress_(std::move(localServerAddress))
        , ShuffleManager_(CreateShuffleManager(std::move(client), std::move(invoker)))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartShuffle));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterChunks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FetchChunks));
        // COMPAT(apollo1321): Remove RegisterMapper after the 26.2 branch is created.
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterMapper));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterWriter));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetPartitionWriteSession));
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, StartShuffle)
    {
        auto parentTransactionId = FromProto<TTransactionId>(request->parent_transaction_id());
        int partitionCount = request->partition_count();
        const auto& account = request->account();
        bool usePushBasedShuffle = request->use_push_based_shuffle();

        auto medium = request->has_medium()
            ? request->medium()
            : DefaultStoreMediumName;
        int replicationFactor = request->has_replication_factor()
            ? request->replication_factor()
            : DefaultIntermediateDataReplicationFactor;

        TTableSchemaPtr schema;
        if (request->has_schema()) {
            FromProto(&schema, request->schema());
        }

        context->SetRequestInfo(
            "ParentTransaction: %v, Account: %v, PartitionCount: %v, Medium: %v, ReplicationFactor: %v, UsePushBasedShuffle: %v",
            parentTransactionId,
            account,
            partitionCount,
            medium,
            replicationFactor,
            usePushBasedShuffle);

        THROW_ERROR_EXCEPTION_IF(
            parentTransactionId.IsEmpty(),
            "Parent transaction id is null");

        THROW_ERROR_EXCEPTION_IF(
            usePushBasedShuffle && !schema,
            "Push-based shuffle requires a schema");

        // Push-based readers reconstruct column names solely from the schema, so a
        // column outside it cannot round-trip. The schema must therefore be strict.
        THROW_ERROR_EXCEPTION_IF(
            usePushBasedShuffle && !schema->IsStrict(),
            "Push-based shuffle requires a strict schema");

        TPushShuffleConfigPtr pushConfig;
        if (request->has_push_config()) {
            pushConfig = ConvertTo<TPushShuffleConfigPtr>(TYsonString(request->push_config()));
        }

        auto transactionId = WaitFor(
            ShuffleManager_->StartShuffle(
                partitionCount,
                parentTransactionId,
                usePushBasedShuffle,
                account,
                medium,
                replicationFactor,
                std::move(pushConfig)))
            .ValueOrThrow();

        auto shuffleHandle = New<TShuffleHandle>();
        shuffleHandle->TransactionId = transactionId;
        shuffleHandle->CoordinatorAddress = LocalServerAddress_;
        shuffleHandle->Account = account;
        shuffleHandle->PartitionCount = partitionCount;
        shuffleHandle->ReplicationFactor = replicationFactor;
        shuffleHandle->Medium = std::move(medium);
        shuffleHandle->UsePushBasedShuffle = usePushBasedShuffle;
        shuffleHandle->Schema = std::move(schema);
        if (request->has_push_config()) {
            shuffleHandle->PushConfig = TYsonString(request->push_config());
        }

        response->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));

        context->SetResponseInfo("TransactionId: %v", shuffleHandle->TransactionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        auto logicalWriterIndex = request->has_logical_writer_index()
            ? std::optional<int>(request->logical_writer_index())
            : std::nullopt;
        bool overwriteExistingWriterData = request->overwrite_existing_writer_data();

        if (overwriteExistingWriterData && !logicalWriterIndex.has_value()) {
            THROW_ERROR_EXCEPTION(
                "Logical writer index must be set when overwrite existing writer data option is enabled");
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, ChunkCount: %v, LogicalWriterIndex: %v, OverwriteExistingWriterData: %v",
            shuffleHandle,
            request->chunk_specs_size(),
            logicalWriterIndex,
            overwriteExistingWriterData);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();
        auto pullController = ToPullBasedOrThrow(controller);

        auto chunks = FromProto<std::vector<TInputChunkPtr>>(request->chunk_specs());

        WaitFor(pullController->RegisterChunks(
            std::move(chunks),
            logicalWriterIndex,
            overwriteExistingWriterData))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FetchChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        std::optional<IShuffleClient::TIndexRange> logicalWriterIndexRange;
        if (request->has_logical_writer_index_range()) {
            const auto& range = request->logical_writer_index_range();
            if (!range.has_begin() || !range.has_end()) {
                THROW_ERROR_EXCEPTION("Logical writer index range begin and end fields are required");
            }

            int begin = range.begin();
            int end = range.end();

            if (begin < 0) {
                THROW_ERROR_EXCEPTION(
                    "Received negative lower limit of logical writer index range %v",
                    begin);
            }
            if (begin > end) {
                THROW_ERROR_EXCEPTION(
                    "Lower limit of logical writer index range %v cannot be greater than upper limit %v",
                    begin,
                    end);
            }

            logicalWriterIndexRange = std::pair(begin, end);
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, PartitionIndex: %v, LogicalWriterIndexRange: %v",
            shuffleHandle,
            request->partition_index(),
            logicalWriterIndexRange);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();

        if (shuffleHandle->UsePushBasedShuffle) {
            auto pushController = ToPushBasedOrThrow(controller);
            auto fetchResult = WaitFor(pushController->FetchChunks(request->partition_index(), logicalWriterIndexRange))
                .ValueOrThrow();
            for (const auto& info : fetchResult.Chunks) {
                auto* protoChunk = response->add_chunk_specs();
                ToProto(protoChunk->mutable_chunk_id(), info.ChunkId);
                ToProto(protoChunk->mutable_replicas(), info.Replicas);
            }
            for (i32 writerId : fetchResult.ValidWriterIds) {
                response->add_valid_writer_ids(writerId);
            }
        } else {
            auto pullController = ToPullBasedOrThrow(controller);
            auto chunkSlices = WaitFor(pullController->FetchChunks(request->partition_index(), logicalWriterIndexRange))
                .ValueOrThrow();
            for (const auto& chunkSlice : chunkSlices) {
                auto* protoChunk = response->add_chunk_specs();
                ToProto(protoChunk, chunkSlice, TComparator(), EDataSourceType::UnversionedTable);
            }
        }

        context->SetResponseInfo("ChunkCount: %v", response->chunk_specs_size());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterWriter)
    {
        DoRegisterWriter(request, response, context);
    }

    // COMPAT(apollo1321): Remove RegisterMapper after the 26.2 branch is created.
    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        NShuffleClient::NProto::TReqRegisterWriter,
        NShuffleClient::NProto::TRspRegisterWriter,
        RegisterMapper)
    {
        DoRegisterWriter(request, response, context);
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, GetPartitionWriteSession)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));
        int partitionIndex = request->partition_index();

        std::optional<NChunkClient::TSessionId> excludedSessionId;
        if (request->has_excluded_session_id()) {
            excludedSessionId = FromProto<NChunkClient::TSessionId>(request->excluded_session_id());
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, PartitionIndex: %v, ExcludedSessionId: %v",
            shuffleHandle,
            partitionIndex,
            excludedSessionId);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();
        auto pushController = ToPushBasedOrThrow(controller);

        auto sessionDescriptor = WaitFor(pushController->GetPartitionWriteSession(partitionIndex, excludedSessionId))
            .ValueOrThrow();

        auto* session = response->mutable_session();
        ToProto(session->mutable_session_id(), sessionDescriptor.SessionId);
        ToProto(session->mutable_sequencer_node(), sessionDescriptor.SequencerNode);

        context->SetResponseInfo("SessionId: %v", sessionDescriptor.SessionId);
        context->Reply();
    }

private:
    const std::string LocalServerAddress_;
    const IShuffleManagerPtr ShuffleManager_;

    void DoRegisterWriter(
        NShuffleClient::NProto::TReqRegisterWriter* request,
        NShuffleClient::NProto::TRspRegisterWriter* response,
        const TCtxRegisterWriterPtr& context)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        auto logicalWriterIndex = request->has_logical_writer_index()
            ? std::optional<int>(request->logical_writer_index())
            : std::nullopt;
        bool overwriteExistingWriterData = request->overwrite_existing_writer_data();

        context->SetRequestInfo(
            "ShuffleHandle: %v, LogicalWriterIndex: %v, OverwriteExistingWriterData: %v",
            shuffleHandle,
            logicalWriterIndex,
            overwriteExistingWriterData);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();
        auto pushController = ToPushBasedOrThrow(controller);

        auto registration = WaitFor(pushController->RegisterWriter(logicalWriterIndex, overwriteExistingWriterData))
            .ValueOrThrow();

        response->set_writer_id(registration.WriterId);
        for (const auto& readySession : registration.ReadySessions) {
            auto* protoSession = response->add_ready_sessions();
            protoSession->set_partition_index(readySession.SlotCookie);
            auto* session = protoSession->mutable_session();
            ToProto(session->mutable_session_id(), readySession.Descriptor.SessionId);
            ToProto(session->mutable_sequencer_node(), readySession.Descriptor.SequencerNode);
        }

        context->SetResponseInfo(
            "WriterId: %v, ReadySessionCount: %v",
            registration.WriterId,
            registration.ReadySessions.size());
        context->Reply();
    }

    static IPullBasedShuffleControllerPtr ToPullBasedOrThrow(const IShuffleControllerPtr& controller)
    {
        auto pullController = DynamicPointerCast<IPullBasedShuffleController>(controller);
        THROW_ERROR_EXCEPTION_IF(!pullController, "This operation is only supported for pull-based shuffles");
        return pullController;
    }

    static IPushBasedShuffleControllerPtr ToPushBasedOrThrow(const IShuffleControllerPtr& controller)
    {
        auto pushController = DynamicPointerCast<IPushBasedShuffleController>(controller);
        THROW_ERROR_EXCEPTION_IF(!pushController, "This operation is only supported for push-based shuffles");
        return pushController;
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateShuffleService(
    IInvokerPtr invoker,
    IClientPtr client,
    std::string localServerAddress)
{
    return New<TShuffleService>(
        std::move(invoker),
        std::move(client),
        std::move(localServerAddress));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
