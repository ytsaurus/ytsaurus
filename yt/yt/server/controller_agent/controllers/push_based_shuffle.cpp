#include "push_based_shuffle.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/push_based_shuffle_registry.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/helpers.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/client/api/config.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NChunkPools;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionPoolPtr CreateSessionPool(
    const TPushBasedShuffleParameters& parameters,
    NNative::IClientPtr client,
    IDistributedChunkSessionSealMonitorPtr sealMonitor,
    IInvokerPtr invoker,
    TLogger logger)
{
    const auto& spec = parameters.Spec;
    const auto& shuffleOptions = parameters.PushBasedShuffleOptions;

    auto writerOptions = New<TJournalChunkWriterOptions>();
    writerOptions->ReplicationFactor = spec->IntermediateDataReplicationFactor;
    auto quorums = ComputeDefaultJournalQuorums(spec->IntermediateDataReplicationFactor);
    writerOptions->ReadQuorum = quorums.ReadQuorum;
    writerOptions->WriteQuorum = quorums.WriteQuorum;

    auto controllerConfig = CloneYsonStruct(shuffleOptions->SessionControllerConfig);
    controllerConfig->Account = spec->IntermediateDataAccount;
    controllerConfig->MediumName = spec->IntermediateDataMediumName;
    controllerConfig->IsVital = false;

    return CreateDistributedChunkSessionPool(
        std::move(client),
        shuffleOptions->SessionPoolConfig,
        std::move(controllerConfig),
        parameters.TransactionId,
        parameters.PartitionCount,
        std::move(writerOptions),
        shuffleOptions->JournalWriterConfig,
        std::move(invoker),
        std::move(sealMonitor),
        std::move(logger));
}

IPushBasedShuffleChunkPoolPtr CreateChunkPool(const TPushBasedShuffleParameters& parameters, TLogger logger)
{
    return CreatePushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions{
        .PartitionCount = parameters.PartitionCount,
        .TargetUncompressedDataSizePerJob = parameters.Spec->DataWeightPerShuffleJob,
        .MaxDataSliceCountPerJob = parameters.Spec->MaxChunkSlicePerShuffleJob,
        .SealFallbackCompressionRatio = parameters.PushBasedShuffleOptions->SealFallbackCompressionRatio,
        .SealFallbackRowCountPerRecord = parameters.PushBasedShuffleOptions->SealFallbackRowCountPerRecord,
        .Logger = std::move(logger),
    });
}

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffle
    : public IPushBasedShuffle
{
public:
    TPushBasedShuffle(
        TPushBasedShuffleParameters parameters,
        TPushBasedShuffleRegistryPtr shuffleRegistry,
        IDistributedChunkSessionSealMonitorPtr sealMonitor,
        NNative::IClientPtr client,
        IInvokerPtr controllerInvoker,
        TCallback<void(std::function<void()>)> invokeSafely,
        TCallback<void(const TError&)> onSessionFailed,
        TCallback<void(int)> onChunkPoolUpdated,
        TLogger logger)
        : Parameters_(std::move(parameters))
        , ShuffleRegistry_(std::move(shuffleRegistry))
        , ControllerInvoker_(std::move(controllerInvoker))
        , InvokeSafely_(std::move(invokeSafely))
        , OnSessionFailed_(std::move(onSessionFailed))
        , OnChunkPoolUpdated_(std::move(onChunkPoolUpdated))
        , Logger(std::move(logger))
        , ChunkPool_(CreateChunkPool(Parameters_, Logger))
        , Pool_(CreateSessionPool(
            Parameters_,
            std::move(client),
            std::move(sealMonitor),
            ControllerInvoker_,
            Logger))
    {
        YT_VERIFY(ControllerInvoker_->IsSerialized());
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControllerInvoker_);

        ShuffleRegistry_->RegisterShuffle(Parameters_.IncarnationId, Parameters_.OperationId, Pool_);

        Pool_->SubscribeProgressUpdated(BIND_NO_PROPAGATE(
            &TPushBasedShuffle::OnSessionProgressUpdated,
            MakeWeak(this)));

        for (int partitionIndex = 0; partitionIndex < Parameters_.PartitionCount; ++partitionIndex) {
            YT_UNUSED_FUTURE(Pool_->GetSession(partitionIndex));
        }
    }

    ~TPushBasedShuffle()
    {
        ShuffleRegistry_->UnregisterShuffle(Parameters_.IncarnationId, Parameters_.OperationId);
    }

    IPushBasedShuffleChunkPoolPtr GetChunkPool() const final
    {
        return ChunkPool_;
    }

    std::vector<TReadySession> GetReadySessions() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Pool_->GetReadySessions();
    }

    void FinalizeSessions() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_UNUSED_FUTURE(Pool_->Finalize());
    }

private:
    const TPushBasedShuffleParameters Parameters_;
    const TPushBasedShuffleRegistryPtr ShuffleRegistry_;
    const IInvokerPtr ControllerInvoker_;
    const TCallback<void(std::function<void()>)> InvokeSafely_;
    const TCallback<void(const TError&)> OnSessionFailed_;
    const TCallback<void(int)> OnChunkPoolUpdated_;
    const TLogger Logger;
    const IPushBasedShuffleChunkPoolPtr ChunkPool_;
    const IDistributedChunkSessionPoolPtr Pool_;

    void OnSessionProgressUpdated(const TSessionProgressUpdate& update)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControllerInvoker_);

        InvokeSafely_.Run([&] {
            ApplySessionProgressUpdate(update);

            if (!std::holds_alternative<TSessionCloseFailed>(update.Progress)) {
                OnChunkPoolUpdated_(update.SlotCookie);
            }
        });
    }

    void ApplySessionProgressUpdate(const TSessionProgressUpdate& update)
    {
        auto chunkId = update.SessionId.ChunkId;

        Visit(update.Progress,
            [&] (const TSessionStarted& started) {
                ChunkPool_->RegisterChunkWriteSession(update.SlotCookie, chunkId, started.Replicas);
            },
            [&] (const TSessionInFlightProgress& inFlight) {
                ChunkPool_->UpdateChunkWriteSession(chunkId, inFlight.Underlying());
            },
            [&] (const TSessionFinalProgress& finalProgress) {
                const auto& progress = finalProgress.Underlying();
                YT_VERIFY(progress);

                ChunkPool_->FinishChunkWriteSession(chunkId, *progress);
            },
            [&] (const TSessionSealSummary& summary) {
                ChunkPool_->FinishChunkWriteSessionFromSeal(chunkId, summary);
            },
            [&] (const TSessionCloseFailed& closeFailed) {
                YT_TLOG_ERROR("Shuffle write session close failed")
                    .With("PartitionIndex", update.SlotCookie)
                    .With("SessionId", update.SessionId)
                    .With(closeFailed.Underlying());

                OnSessionFailed_(TError(
                    "Shuffle write session %v of partition %v could not be closed or sealed",
                    update.SessionId,
                    update.SlotCookie)
                    .With(closeFailed.Underlying()));
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPushBasedShufflePtr CreatePushBasedShuffle(
    TPushBasedShuffleParameters parameters,
    TPushBasedShuffleRegistryPtr shuffleRegistry,
    IDistributedChunkSessionSealMonitorPtr sealMonitor,
    NNative::IClientPtr client,
    IInvokerPtr controllerInvoker,
    TCallback<void(std::function<void()>)> invokeSafely,
    TCallback<void(const TError&)> onSessionFailed,
    TCallback<void(int)> onChunkPoolUpdated,
    TLogger logger)
{
    return New<TPushBasedShuffle>(
        std::move(parameters),
        std::move(shuffleRegistry),
        std::move(sealMonitor),
        std::move(client),
        std::move(controllerInvoker),
        std::move(invokeSafely),
        std::move(onSessionFailed),
        std::move(onChunkPoolUpdated),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
