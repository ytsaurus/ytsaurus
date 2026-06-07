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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterMapper));
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

        auto writerIndex = request->has_writer_index() ? std::optional<int>(request->writer_index()) : std::nullopt;
        bool overwriteExistingWriterData = request->overwrite_existing_writer_data();

        if (overwriteExistingWriterData && !writerIndex.has_value()) {
            THROW_ERROR_EXCEPTION("Writer index must be set when overwrite existing writer data option is enabled");
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, ChunkCount: %v, MapperId: %v, OverwriteExistingWriterData: %v",
            shuffleHandle,
            request->chunk_specs_size(),
            writerIndex,
            overwriteExistingWriterData);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();
        auto pullController = ToPullBasedOrThrow(controller);

        auto chunks = FromProto<std::vector<TInputChunkPtr>>(request->chunk_specs());

        WaitFor(pullController->RegisterChunks(
            std::move(chunks),
            writerIndex,
            overwriteExistingWriterData))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FetchChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        std::optional<IShuffleClient::TIndexRange> writerIndexRange;
        if (request->has_writer_index_range()) {
            if (!request->writer_index_range().has_begin() || !request->writer_index_range().has_end()) {
                THROW_ERROR_EXCEPTION("Writer index range begin and end fields are required");
            }

            int writerIndexBegin = request->writer_index_range().begin();
            int writerIndexEnd = request->writer_index_range().end();

            if (writerIndexBegin < 0) {
                THROW_ERROR_EXCEPTION("Received negative lower limit of writer index range %v", writerIndexBegin);
            }

            if (writerIndexBegin > writerIndexEnd) {
                THROW_ERROR_EXCEPTION(
                    "Lower limit of mappers range %v cannot be greater than upper limit %v",
                    writerIndexBegin,
                    writerIndexEnd);
            }

            writerIndexRange = std::pair(writerIndexBegin, writerIndexEnd);
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, PartitionIndex: %v, WriterIndexRange: %v",
            shuffleHandle,
            request->partition_index(),
            writerIndexRange);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();

        if (auto pushController = DynamicPointerCast<IPushBasedShuffleController>(controller)) {
            auto fetchResult = WaitFor(pushController->FetchChunks(request->partition_index(), writerIndexRange))
                .ValueOrThrow();
            for (const auto& info : fetchResult.Chunks) {
                auto* protoChunk = response->add_chunk_specs();
                ToProto(protoChunk->mutable_chunk_id(), info.ChunkId);
                ToProto(protoChunk->mutable_replicas(), info.Replicas);
            }
            for (i32 mapperId : fetchResult.ValidMapperIds) {
                response->add_valid_mapper_ids(mapperId);
            }
        } else {
            auto pullController = ToPullBasedOrThrow(controller);
            auto chunkSlices = WaitFor(pullController->FetchChunks(request->partition_index(), writerIndexRange))
                .ValueOrThrow();
            for (const auto& chunkSlice : chunkSlices) {
                auto* protoChunk = response->add_chunk_specs();
                ToProto(protoChunk, chunkSlice, TComparator(), EDataSourceType::UnversionedTable);
            }
        }

        context->SetResponseInfo("ChunkCount: %v", response->chunk_specs_size());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterMapper)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        auto writerIndex = request->has_writer_index()
            ? std::optional<int>(request->writer_index())
            : std::nullopt;
        bool overwrite = request->overwrite_existing_writer_data();

        context->SetRequestInfo(
            "ShuffleHandle: %v, WriterIndex: %v, OverwriteExistingWriterData: %v",
            shuffleHandle,
            writerIndex,
            overwrite);

        auto controller = WaitFor(ShuffleManager_->GetController(shuffleHandle->TransactionId))
            .ValueOrThrow();
        auto pushController = ToPushBasedOrThrow(controller);

        auto registration = WaitFor(pushController->RegisterMapper(writerIndex, overwrite))
            .ValueOrThrow();

        response->set_mapper_id(registration.MapperId);
        for (const auto& readySession : registration.ReadySessions) {
            auto* protoSession = response->add_ready_sessions();
            protoSession->set_partition_index(readySession.SlotCookie);
            auto* session = protoSession->mutable_session();
            ToProto(session->mutable_session_id(), readySession.Descriptor.SessionId);
            ToProto(session->mutable_sequencer_node(), readySession.Descriptor.SequencerNode);
        }

        context->SetResponseInfo(
            "MapperId: %v, ReadySessionCount: %v",
            registration.MapperId,
            registration.ReadySessions.size());
        context->Reply();
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
