#include "shuffle_service.h"

#include "private.h"
#include "shuffle_manager.h"

#include <yt/yt/ytlib/shuffle_client/shuffle_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/client/api/shuffle_client.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
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
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, StartShuffle)
    {
        auto parentTransactionId = FromProto<TTransactionId>(request->parent_transaction_id());
        int partitionCount = request->partition_count();
        const auto& account = request->account();

        context->SetRequestInfo(
            "ParentTransaction: %v, Account: %v, PartitionCount: %v",
            parentTransactionId,
            account,
            partitionCount);

        THROW_ERROR_EXCEPTION_IF(
            parentTransactionId.IsEmpty(),
            "Parent transaction id is null");

        auto transactionId = WaitFor(
            ShuffleManager_->StartShuffle(request->partition_count(), parentTransactionId))
            .ValueOrThrow();

        auto shuffleHandle = New<TShuffleHandle>();
        shuffleHandle->TransactionId = transactionId;
        shuffleHandle->CoordinatorAddress = LocalServerAddress_;
        shuffleHandle->Account = account;
        shuffleHandle->PartitionCount = partitionCount;
        shuffleHandle->ReplicationFactor = request->has_replication_factor()
            ? request->replication_factor()
            : DefaultIntermediateDataReplicationFactor;
        shuffleHandle->Medium = request->has_medium()
            ? request->medium()
            : DefaultStoreMediumName;

        response->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));

        context->SetResponseInfo("TransactionId: %v", shuffleHandle->TransactionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        auto writerIndex = request->has_writer_index() ? std::optional<int>(request->writer_index()) : std::nullopt;
        bool overwriteExistingWriterData = request->overwrite_existing_writer_data();

        YT_VERIFY(!overwriteExistingWriterData || writerIndex.has_value());

        context->SetRequestInfo(
            "ShuffleHandle: %v, ChunkCount: %v, MapperId: %v, OverwriteExistingWriterData: %v",
            shuffleHandle,
            request->chunk_specs_size(),
            writerIndex,
            overwriteExistingWriterData);

        auto chunks = FromProto<std::vector<TInputChunkPtr>>(request->chunk_specs());

        WaitFor(ShuffleManager_->RegisterChunks(
            shuffleHandle->TransactionId,
            chunks,
            writerIndex,
            overwriteExistingWriterData))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FetchChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        std::optional<std::pair<int, int>> writerIndexRange;
        if (request->has_writer_index_range()) {
            YT_VERIFY(request->writer_index_range().has_begin() && request->writer_index_range().has_end());

            writerIndexRange = std::pair(request->writer_index_range().begin(), request->writer_index_range().end());
            YT_VERIFY(writerIndexRange->first >= 0);
            YT_VERIFY(writerIndexRange->first <= writerIndexRange->second);
        }

        context->SetRequestInfo(
            "ShuffleHandle: %v, PartitionIndex: %v, WriterIndexRange: %v",
            shuffleHandle,
            request->partition_index(),
            writerIndexRange);

        auto chunks = WaitFor(ShuffleManager_->FetchChunks(
            shuffleHandle->TransactionId,
            request->partition_index(),
            writerIndexRange))
            .ValueOrThrow();

        for (const auto& chunk : chunks) {
            auto* protoChunk = response->add_chunk_specs();
            ToProto(protoChunk, chunk, TComparator(), EDataSourceType::UnversionedTable);
        }

        context->SetResponseInfo("ChunkCount: %v", response->chunk_specs_size());

        context->Reply();
    }

private:
    const std::string LocalServerAddress_;
    const IShuffleManagerPtr ShuffleManager_;
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
