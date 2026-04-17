#include "distributed_chunk_session_controller.h"

#include "config.h"
#include "distributed_chunk_session_service_proxy.h"
#include "private.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <util/random/shuffle.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYson;

using NApi::NNative::IClientPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// NB: We deliberately avoid WaitFor throughout this file to reduce fiber stack
// memory consumption. Each fiber that at least once blocked in WaitFor
// occupies stack space (typically 256KB). With a large number of concurrent
// sessions this adds up significantly. Using future pipelines instead allows
// the same logical flow without holding a fiber while waiting for I/O.

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControllerState,
    (Created)
    (Starting)
    (Running)
    (Closing)
    (Closed)
);

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionController
    : public IDistributedChunkSessionController
{
public:
    TDistributedChunkSessionController(
        IClientPtr client,
        TDistributedChunkSessionControllerConfigPtr config,
        TTransactionId transactionId,
        TJournalChunkWriterOptionsPtr writerOptions,
        TJournalChunkWriterConfigPtr writerConfig,
        IInvokerPtr invoker)
        : Client_(std::move(client))
        , Config_(std::move(config))
        , TransactionId_(transactionId)
        , WriterOptions_(std::move(writerOptions))
        , WriterConfig_(std::move(writerConfig))
        , Invoker_(std::move(invoker))
        , Logger(DistributedChunkSessionLogger().WithTag("TransactionId: %v", TransactionId_))
    { }

    TFuture<TNodeDescriptor> StartSession() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TransitionState(EControllerState::Created, EControllerState::Starting);

        return CreateChunk()
            .Apply(BIND(
                &TDistributedChunkSessionController::StartRemoteSession,
                MakeStrong(this)))
            .Apply(BIND(
                &TDistributedChunkSessionController::InitializePingExecutor,
                MakeStrong(this)))
            .Apply(BIND(
                &TDistributedChunkSessionController::HandleStartResult,
                MakeStrong(this)));
    }

    TFuture<void> Close() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        CloseSession();
        return ClosedPromise_.ToFuture();
    }

    TFuture<void> GetClosedFuture() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ClosedPromise_.ToFuture();
    }

    TSessionId GetSessionId() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto state = State_.load();
        YT_LOG_FATAL_IF(
            state != EControllerState::Running && state != EControllerState::Closed,
            "Unexpected controller state (State: %v)",
            state);

        return SessionId_;
    }

private:
    const IClientPtr Client_;
    const TDistributedChunkSessionControllerConfigPtr Config_;
    const TTransactionId TransactionId_;

    const TJournalChunkWriterOptionsPtr WriterOptions_;
    const TJournalChunkWriterConfigPtr WriterConfig_;

    const IInvokerPtr Invoker_;

    const TLogger Logger;

    std::atomic<EControllerState> State_ = EControllerState::Created;

    TPeriodicExecutorPtr SessionPingExecutor_;
    TChunkReplicaWithMediumList ChunkReplicas_;

    TSessionId SessionId_;
    TChunkReplicaWithMediumList Targets_;

    TNodeDescriptor SequencerDescriptor_;
    IChannelPtr SequencerChannel_;

    const TPromise<void> ClosedPromise_ = NewPromise<void>();

    int ConsecutivePingFailures_ = 0;

    TFuture<TSessionId> CreateChunk() const
    {
        auto channel = Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(TransactionId_));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.CreateChunk();
        req->SetTimeout(Config_->CreateChunkTimeout);
        GenerateMutationId(req);

        req->set_type(ToProto(EObjectType::JournalChunk));
        req->set_account(Config_->Account);
        ToProto(req->mutable_transaction_id(), TransactionId_);
        req->set_replication_factor(WriterOptions_->ReplicationFactor);
        req->set_erasure_codec(ToProto(WriterOptions_->ErasureCodec));
        req->set_medium_name(Config_->MediumName);
        req->set_read_quorum(WriterOptions_->ReadQuorum);
        req->set_write_quorum(WriterOptions_->WriteQuorum);
        req->set_movable(true);
        req->set_vital(Config_->IsVital);

        return req->Invoke().Apply(BIND([] (const TChunkServiceProxy::TErrorOrRspCreateChunkPtr& rspOrError) {
            return FromProto<TSessionId>(rspOrError.ValueOrThrow()->session_id());
        }));
    }

    TFuture<void> StartRemoteSession(TSessionId sessionId)
    {
        SessionId_ = sessionId;
        YT_LOG_INFO("Chunk created (ChunkId: %v)", SessionId_);

        // TODO(apollo1321): AllocateWriteTargets uses WaitFor internally, which contradicts
        // the no-WaitFor design of this file. Write targets allocation should also be batched
        // to reduce master workload. Both should be fixed via a distributed chunk session pool.
        Targets_ = AllocateWriteTargets(
            Client_,
            SessionId_,
            /*desiredTargetCount*/ WriterOptions_->ReplicationFactor,
            /*minTargetCount*/ WriterOptions_->ReplicationFactor,
            /*replicationFactorOverride*/ {},
            /*preferredHostName*/ {},
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger);

        Shuffle(Targets_.begin(), Targets_.end());

        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
        const auto& channelFactory = Client_->GetChannelFactory();
        const auto& networks = Client_->GetNativeConnection()->GetNetworks();

        SequencerDescriptor_ = nodeDirectory->GetDescriptor(Targets_[0]);
        SequencerChannel_ = channelFactory->CreateChannel(
            SequencerDescriptor_.GetAddressOrThrow(networks));

        YT_LOG_INFO(
            "Selected sequencer node (Address: %v)",
            SequencerDescriptor_.GetAddressOrThrow(networks));

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.StartSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_session_timeout(ToProto(Config_->SessionTimeout));
        ToProto(req->mutable_chunk_replicas(), Targets_);
        req->set_journal_chunk_writer_options(ToProto(ConvertToYsonString(WriterOptions_)));
        req->set_journal_chunk_writer_config(ToProto(ConvertToYsonString(WriterConfig_)));

        return req->Invoke().AsVoid();
    }

    TNodeDescriptor InitializePingExecutor(const TError& error)
    {
        error.ThrowOnError();

        SessionPingExecutor_ = New<TPeriodicExecutor>(
            Invoker_,
            BIND_NO_PROPAGATE(&TDistributedChunkSessionController::SendSequencerPing, MakeWeak(this)),
            Config_->SessionPingPeriod);

        SessionPingExecutor_->Start();

        TransitionState(EControllerState::Starting, EControllerState::Running);

        return SequencerDescriptor_;
    }

    TNodeDescriptor HandleStartResult(const TErrorOr<TNodeDescriptor>& descriptorOrError)
    {
        if (!descriptorOrError.IsOK()) {
            auto error = TError(descriptorOrError);
            YT_LOG_DEBUG(error, "Failed to start session");
            TransitionState(EControllerState::Starting, EControllerState::Closed);
            ClosedPromise_.Set(error);
            descriptorOrError.ThrowOnError();
        }

        return descriptorOrError.Value();
    }

    void SendSequencerPing()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG(
            "Sending sequencer ping (Address: %v)",
            SequencerDescriptor_.GetDefaultAddress());

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.PingSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        req->Invoke()
            .AsVoid()
            .Subscribe(BIND(
                &TDistributedChunkSessionController::OnSequencerPingResponse,
                MakeWeak(this))
                .Via(Invoker_));
    }

    void OnSequencerPingResponse(const TError& error)
    {
        if (error.IsOK()) {
            YT_LOG_DEBUG("Successfully pinged session");
            ConsecutivePingFailures_ = 0;
            return;
        }

        if (error.GetCode() == NChunkClient::EErrorCode::NoSuchSession) {
            YT_LOG_DEBUG(error, "Session has expired, finishing controller");

            auto expected = EControllerState::Running;
            if (State_.compare_exchange_strong(expected, EControllerState::Closed)) {
                ClosedPromise_.SetFrom(
                    StopPingExecutor().Apply(BIND([error] {
                        return MakeFuture(error);
                    })));
            }
            return;
        }

        ++ConsecutivePingFailures_;
        YT_LOG_DEBUG(
            error,
            "Session ping failed (ConsecutivePingFailures: %v, MaxConsecutivePingFailures: %v)",
            ConsecutivePingFailures_,
            Config_->MaxConsecutivePingFailures);

        if (ConsecutivePingFailures_ >= Config_->MaxConsecutivePingFailures) {
            YT_LOG_DEBUG(error, "Too many consecutive ping failures, finishing controller");

            auto expected = EControllerState::Running;
            if (State_.compare_exchange_strong(expected, EControllerState::Closed)) {
                ClosedPromise_.SetFrom(
                    StopPingExecutor().Apply(BIND([error] {
                        return MakeFuture(error.Wrap("Too many consecutive ping failures"));
                    })));
            }
        }
    }

    void CloseSession()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto expected = EControllerState::Running;
        if (!State_.compare_exchange_strong(expected, EControllerState::Closing)) {
            YT_LOG_DEBUG("Session is not running (State: %v)", expected);

            YT_LOG_FATAL_IF(
                expected != EControllerState::Closing && expected != EControllerState::Closed,
                "Unexpected controller state (State: %v)",
                expected);
            return;
        }

        YT_LOG_DEBUG("Closing session");

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.FinishSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        ClosedPromise_.SetFrom(
            req->Invoke()
                .AsVoid()
                .AsUnique()
                .Apply(BIND(
                    &TDistributedChunkSessionController::OnSessionFinished,
                    MakeStrong(this)))
                .AsUnique()
                .Apply(BIND(
                    &TDistributedChunkSessionController::OnPingExecutorStopped,
                    MakeStrong(this))));
    }

    TUniqueFuture<void> OnSessionFinished(TError&& error)
    {
        if (error.IsOK()) {
            YT_LOG_DEBUG("Successfully closed session");
        } else {
            YT_LOG_DEBUG(error, "Error occurred while closing session");
        }

        return StopPingExecutor().Apply(BIND([error] {
            return MakeFuture(error);
        })).AsUnique();
    }

    void OnPingExecutorStopped(TError&& finishError)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TransitionState(EControllerState::Closing, EControllerState::Closed);

        finishError.ThrowOnError();
    }

    TUniqueFuture<void> StopPingExecutor()
    {
        return SessionPingExecutor_->Stop()
            .Apply(BIND([Logger = Logger] (const TError& error) {
                YT_LOG_FATAL_IF(
                    !error.IsOK(),
                    error,
                    "Unexpected failure during session ping executor stopping");
            }))
            .AsUnique();
    }

    void TransitionState(EControllerState from, EControllerState to)
    {
        auto actual = State_.exchange(to);
        YT_LOG_FATAL_IF(
            actual != from,
            "Unexpected controller state (ExpectedState: %v, ActualState: %v, NewState: %v)",
            from,
            actual,
            to);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IDistributedChunkSessionControllerPtr CreateDistributedChunkSessionController(
    IClientPtr client,
    TDistributedChunkSessionControllerConfigPtr config,
    TTransactionId transactionId,
    TJournalChunkWriterOptionsPtr writerOptions,
    TJournalChunkWriterConfigPtr writerConfig,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkSessionController>(
        std::move(client),
        std::move(config),
        transactionId,
        std::move(writerOptions),
        std::move(writerConfig),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
