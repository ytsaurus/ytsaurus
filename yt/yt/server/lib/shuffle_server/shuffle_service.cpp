#include "shuffle_service.h"

#include "private.h"
#include "shuffle_manager.h"

#include <yt/yt/ytlib/shuffle_client/shuffle_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/client/api/shuffle_client.h>

#include <yt/yt/core/rpc/service_detail.h>

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

        context->SetRequestInfo(
            "PartitionCount: %v, Account: %v, ParentTransaction: %v",
            request->partition_count(),
            request->account(),
            parentTransactionId);

        THROW_ERROR_EXCEPTION_IF(
            parentTransactionId.IsEmpty(),
            "Parent transaction id is null");

        auto transactionId = WaitFor(
            ShuffleManager_->StartShuffle(request->partition_count(), parentTransactionId))
            .ValueOrThrow();

        auto shuffleHandle = New<TShuffleHandle>();
        shuffleHandle->TransactionId = transactionId;
        shuffleHandle->CoordinatorAddress = LocalServerAddress_;
        shuffleHandle->Account = request->account();
        shuffleHandle->PartitionCount = request->partition_count();
        shuffleHandle->ReplicationFactor = request->has_replication_factor()
            ? request->replication_factor()
            : DefaultIntermediateDataReplicationFactor;
        shuffleHandle->MediumName = request->has_medium_name()
            ? request->medium_name()
            : DefaultStoreMediumName;

        response->set_shuffle_handle(ConvertToYsonString(shuffleHandle).ToString());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, RegisterChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        context->SetRequestInfo(
            "TransactionId: %v, CoordinatorAddress: %v, Account: %v, PartitionCount: %v, ChunkCount: %v",
            shuffleHandle->TransactionId,
            shuffleHandle->CoordinatorAddress,
            shuffleHandle->Account,
            shuffleHandle->PartitionCount,
            request->chunk_specs_size());

        auto chunks = FromProto<std::vector<TInputChunkPtr>>(request->chunk_specs());

        WaitFor(ShuffleManager_->RegisterChunks(
            shuffleHandle->TransactionId,
            chunks))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NShuffleClient::NProto, FetchChunks)
    {
        auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(request->shuffle_handle()));

        context->SetRequestInfo(
            "TransactionId: %v, CoordinatorAddress: %v, Account: %v, PartitionCount: %v, PartitionIndex: %v",
            shuffleHandle->TransactionId,
            shuffleHandle->CoordinatorAddress,
            shuffleHandle->Account,
            shuffleHandle->PartitionCount,
            request->partition_index());

        auto chunks = WaitFor(ShuffleManager_->FetchChunks(
            shuffleHandle->TransactionId,
            request->partition_index()))
            .ValueOrThrow();

        for (const auto& chunk : chunks) {
            auto* protoChunk = response->add_chunk_specs();
            ToProto(protoChunk, chunk, TComparator(), EDataSourceType::UnversionedTable);
        }

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
