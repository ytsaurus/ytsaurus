#include "session_controller.h"

#include "config.h"
#include "service_proxy.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <util/random/random.h>

#include <optional>
#include <utility>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

using NApi::NNative::IClientPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// NB: We deliberately avoid WaitFor throughout this file to reduce fiber stack
// memory consumption. Each fiber that at least once blocked in WaitFor
// occupies stack space (typically 256KB). With a large number of concurrent
// sessions this adds up significantly. Using future pipelines instead allows
// the same logical flow without holding a fiber while waiting for I/O.

////////////////////////////////////////////////////////////////////////////////

//! Session state machine. Entering Closed raises the terminal alternative, so exactly
//! one is raised no matter which path ends the session.
/*!
 *    +---------+
 *    | Created |
 *    +---------+
 *         | StartSession()
 *         v
 *    +----------+
 *    | Starting | --- start failure ---------------->|  raise CloseFailed
 *    +----------+                                    |
 *         | ping executor started                    |
 *         v                                          |
 *    +---------+ --.                                 |
 *    | Running |   | ping response -> raise InFlight |
 *    +---------+ <-'                                 |
 *         |  '--- ping lost / max failures --------->|  raise CloseFailed
 *         | Close()                                  |
 *         v                                          |
 *    +---------+ --.                                 |
 *    | Closing |   | ping response -> raise InFlight |
 *    +---------+ <-'  at most once                   |
 *         '--- FinishSession replied --------------->|  raise Final or CloseFailed
 *                                                    |
 *                                                    v
 *                                               +--------+
 *                                               | Closed |
 *                                               +--------+
 */
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
        , Invoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(DistributedChunkSessionLogger().WithTag("TransactionId", TransactionId_))
    { }

    TFuture<TStartedSessionInfo> StartSession() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        TransitionState(EControllerState::Created, EControllerState::Starting);

        // NB: StartRemoteSession blocks in WaitFor (inside AllocateWriteTargets; see
        // the TODO there). Without AsyncVia it would run inline in the CreateChunk
        // future handler and suspend whoever called Set on that future, which is why
        // context switches in future handlers are forbidden. May be removed once the
        // TODO is resolved.
        return CreateChunk()
            .Apply(BIND(
                &TDistributedChunkSessionController::StartRemoteSession,
                MakeStrong(this))
                .AsyncVia(Invoker_))
            .Apply(BIND(
                &TDistributedChunkSessionController::OnSessionStarted,
                MakeStrong(this))
                .AsyncVia(Invoker_))
            .ToUncancelable();
    }

    TFuture<void> Close() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        CloseSession();
        return ClosedFuture_;
    }

    TFuture<void> GetClosedFuture() final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ClosedFuture_;
    }

    TSessionId GetSessionId() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return SessionId_.Load();
    }

    //! NB: The terminal alternative may be raised before the pool subscribes, since pings
    //! start while StartSession() is still resolving. TerminalProgressUpdated_ replays it
    //! to late subscribers, so it is delivered exactly once either way.
    void SubscribeProgressUpdated(const TCallback<TSessionProgressUpdatedSignature>& callback) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        InFlightProgressUpdated_.Subscribe(callback);
        TerminalProgressUpdated_.Subscribe(callback);
    }

    void UnsubscribeProgressUpdated(const TCallback<TSessionProgressUpdatedSignature>& callback) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        InFlightProgressUpdated_.Unsubscribe(callback);
        TerminalProgressUpdated_.Unsubscribe(callback);
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

    NThreading::TAtomicObject<TSessionId> SessionId_;
    TChunkReplicaWithMediumList Targets_;

    TNodeDescriptor SequencerDescriptor_;
    IChannelPtr SequencerChannel_;

    const TPromise<void> ClosedPromise_ = NewPromise<void>();
    const TFuture<void> ClosedFuture_ = ClosedPromise_.ToFuture().ToUncancelable();

    std::optional<TDistributedChunkSessionProgress> Progress_;

    TCallbackList<TSessionProgressUpdatedSignature> InFlightProgressUpdated_;
    TSingleShotCallbackList<TSessionProgressUpdatedSignature> TerminalProgressUpdated_;

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
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        SessionId_.Store(sessionId);
        YT_TLOG_INFO("Chunk created")
            .With("ChunkId", sessionId);

        // TODO(apollo1321): AllocateWriteTargets uses WaitFor internally, which contradicts
        // the no-WaitFor design of this file. Write targets allocation should also be batched
        // to reduce master workload. Both should be fixed via a distributed chunk session pool.
        Targets_ = AllocateWriteTargets(
            Client_,
            sessionId,
            /*desiredTargetCount*/ WriterOptions_->ReplicationFactor,
            /*minTargetCount*/ WriterOptions_->ReplicationFactor,
            /*replicationFactorOverride*/ {},
            /*preferredHostName*/ {},
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger);

        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
        const auto& channelFactory = Client_->GetChannelFactory();
        const auto& networks = Client_->GetNativeConnection()->GetNetworks();

        SequencerDescriptor_ = nodeDirectory->GetDescriptor(Targets_[RandomNumber(Targets_.size())]);
        SequencerChannel_ = channelFactory->CreateChannel(
            SequencerDescriptor_.GetAddressOrThrow(networks));

        YT_TLOG_INFO("Selected sequencer node")
            .With("Address", SequencerDescriptor_.GetAddressOrThrow(networks));

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.StartSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_session_timeout(ToProto(Config_->SessionTimeout));
        ToProto(req->mutable_chunk_replicas(), Targets_);
        req->set_journal_chunk_writer_options(ToProto(ConvertToYsonString(WriterOptions_)));
        req->set_journal_chunk_writer_config(ToProto(ConvertToYsonString(WriterConfig_)));

        return req->Invoke().AsVoid();
    }

    TStartedSessionInfo OnSessionStarted(const TError& startError)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        // NB: Every failure has to be observed while the controller is still starting,
        // since a session left in that state neither terminates nor accepts Close().
        auto startedSessionOrError = [&] () -> TErrorOr<TStartedSessionInfo> {
            try {
                startError.ThrowOnError();

                TStartedSessionInfo startedSession{
                    .SessionId = SessionId_.Load(),
                    .SequencerNode = SequencerDescriptor_,
                    .Replicas = Targets_,
                };

                SessionPingExecutor_ = New<TPeriodicExecutor>(
                    Invoker_,
                    BIND_NO_PROPAGATE(&TDistributedChunkSessionController::SendSequencerPing, MakeWeak(this)),
                    Config_->SessionPingPeriod);
                SessionPingExecutor_->Start();

                TransitionState(EControllerState::Starting, EControllerState::Running);

                return startedSession;
            } catch (const std::exception& ex) {
                return TError(ex);
            }
        }();

        if (!startedSessionOrError.IsOK()) {
            const TError& error = startedSessionOrError;
            YT_TLOG_DEBUG("Failed to start session")
                .With(error);
            YT_VERIFY(TryTerminate(EControllerState::Starting, error));
            ClosedPromise_.Set(error);
        }

        return startedSessionOrError.ValueOrThrow();
    }

    void SendSequencerPing() noexcept
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        YT_TLOG_DEBUG("Sending sequencer ping")
            .With("Address", SequencerDescriptor_.GetDefaultAddress());

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.PingSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_.Load());

        req->Invoke()
            .Subscribe(BIND(
                &TDistributedChunkSessionController::OnSequencerPingResponse,
                MakeWeak(this))
                .Via(Invoker_));
    }

    void OnSequencerPingResponse(
        const TDistributedChunkSessionServiceProxy::TErrorOrRspPingSessionPtr& responseOrError) noexcept
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        // NB: The ping executor is stopped asynchronously, so responses may still arrive
        // after the session started closing. The finish response is its last word.
        auto state = State_.load();
        if (state != EControllerState::Running) {
            YT_TLOG_DEBUG("Ignoring ping response of a non-running session")
                .With("State", state);
            return;
        }

        if (!responseOrError.IsOK()) {
            const TError& error = responseOrError;

            if (error.GetCode() == NChunkClient::EErrorCode::NoSuchSession) {
                YT_TLOG_DEBUG("Session has been lost or expired, finishing controller")
                    .With(error);

                CloseWithError(error);
                return;
            }

            ++ConsecutivePingFailures_;
            YT_TLOG_DEBUG("Session ping failed")
                .With("ConsecutivePingFailures", ConsecutivePingFailures_)
                .With("MaxConsecutivePingFailures", Config_->MaxConsecutivePingFailures)
                .With(error);

            if (ConsecutivePingFailures_ >= Config_->MaxConsecutivePingFailures) {
                YT_TLOG_DEBUG("Too many consecutive ping failures, finishing controller")
                    .With(error);

                CloseWithError(error.Wrap("Too many consecutive ping failures"));
            }
            return;
        }

        const auto& response = responseOrError.Value();

        bool progressUpdated = false;
        // COMPAT(apollo1321): A pre-26.2 sequencer reports no progress. Progress_ has to
        // stay empty, since an engaged zero value would be published as an exact terminal
        // result and suppress the master-seal fallback.
        if (response->has_progress()) {
            auto progressUpdatedOrError = TryUpdateProgress(
                FromProto<TDistributedChunkSessionProgress>(response->progress()),
                /*isFinal*/ false);
            if (!progressUpdatedOrError.IsOK()) {
                CloseWithError(progressUpdatedOrError);
                return;
            }

            progressUpdated = progressUpdatedOrError.Value();
        }

        YT_TLOG_DEBUG("Successfully pinged session");
        ConsecutivePingFailures_ = 0;

        // NB: Subscribers run inline here and must not throw.
        if (progressUpdated) {
            InFlightProgressUpdated_.Fire(TSessionInFlightProgress(*Progress_));
        }
    }

    void CloseWithError(const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (!TryTerminate(EControllerState::Running, error)) {
            return;
        }

        ClosedPromise_.SetFrom(
            StopPingExecutor().Apply(BIND([error] {
                return MakeFuture(error);
            })));
    }

    //! Returns false when the move to the terminal state was lost to a concurrent one.
    bool TryTerminate(EControllerState from, const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (TryTransitionState(from, EControllerState::Closed) != from) {
            return false;
        }

        TerminalProgressUpdated_.Fire(error.IsOK()
            ? TControllerSessionProgress(TSessionFinalProgress(Progress_))
            : TControllerSessionProgress(TSessionCloseFailed(error)));

        // NB: No in-flight update can follow the terminal one, so the subscribers are
        // released rather than held until the controller dies.
        InFlightProgressUpdated_.Clear();

        return true;
    }

    void CloseSession()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto actual = TryTransitionState(EControllerState::Running, EControllerState::Closing);
        if (actual != EControllerState::Running) {
            YT_TLOG_DEBUG("Session is not running")
                .With("State", actual);

            YT_TLOG_FATAL_IF(
                actual != EControllerState::Closing && actual != EControllerState::Closed,
                "Session is closed before it has started")
                .With("State", actual);
            return;
        }

        YT_TLOG_DEBUG("Closing session");

        TDistributedChunkSessionServiceProxy proxy(SequencerChannel_);
        auto req = proxy.FinishSession();
        req->SetTimeout(Config_->NodeRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_.Load());

        ClosedPromise_.SetFrom(
            req->Invoke()
                .Apply(BIND(
                    &TDistributedChunkSessionController::OnSessionFinished,
                    MakeStrong(this))
                    .AsyncVia(Invoker_)));
    }

    //! Runs on both the success and the failure path: the ping executor must be stopped
    //! and the state transitioned even when the finish RPC or progress publication failed.
    TFuture<void> OnSessionFinished(
        const TDistributedChunkSessionServiceProxy::TErrorOrRspFinishSessionPtr& responseOrError)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        TError error = responseOrError;
        // COMPAT(apollo1321): Otherwise the sequencer is pre-26.2 and Progress_ stays
        // empty, which terminates the session without any logical counters.
        if (error.IsOK() && responseOrError.Value()->has_progress()) {
            error = TryUpdateProgress(
                FromProto<TDistributedChunkSessionProgress>(responseOrError.Value()->progress()),
                /*isFinal*/ true);
        }

        if (error.IsOK()) {
            YT_TLOG_DEBUG("Successfully closed session");
        } else {
            YT_TLOG_DEBUG("Error occurred while closing session")
                .With(error);
        }

        YT_VERIFY(TryTerminate(EControllerState::Closing, error));

        return StopPingExecutor().Apply(BIND([error] {
            error.ThrowOnError();
        }));
    }

    TFuture<void> StopPingExecutor()
    {
        return SessionPingExecutor_->Stop()
            .Apply(BIND([Logger = Logger] (const TError& error) {
                YT_TLOG_FATAL_IF(!error.IsOK(), "Unexpected failure during session ping executor stopping")
                    .With(error);
            }));
    }

    //! A sequencer reporting impossible progress is a bug, so the caller fails the session
    //! instead of retrying against a peer that will keep sending the same thing.
    TError OnBrokenProgress(TError error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        YT_TLOG_ALERT("Sequencer reported broken session progress")
            .With(error);

        return error;
    }

    //! Returns whether the progress advanced.
    TErrorOr<bool> TryUpdateProgress(const TDistributedChunkSessionProgress& progress, bool isFinal)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto state = State_.load();
        YT_VERIFY(state == EControllerState::Running || state == EControllerState::Closing);

        if (!IsNonnegative(progress)) {
            return OnBrokenProgress(
                TError("Distributed chunk session progress must be nonnegative")
                    .With("progress", progress));
        }

        if (Progress_ && IsComponentwiseLessOrEqual(progress, *Progress_)) {
            // NB: A ping may trail confirmed progress, since responses can arrive out of
            // order, but the single final response is the sequencer's last word.
            if (isFinal && progress != *Progress_) {
                return OnBrokenProgress(
                    TError("Final distributed chunk session progress is behind "
                        "previously confirmed progress")
                        .With("confirmed_progress", *Progress_)
                        .With("final_progress", progress));
            }
            return false;
        }

        if (Progress_ && !IsComponentwiseLessOrEqual(*Progress_, progress)) {
            return OnBrokenProgress(
                TError("Distributed chunk session progress counters changed inconsistently")
                    .With("current_progress", *Progress_)
                    .With("new_progress", progress));
        }

        Progress_ = progress;
        YT_TLOG_DEBUG("Session progress updated")
            .With("Progress", progress);
        return true;
    }

    //! The single writer of State_. Returns the state it actually observed.
    EControllerState TryTransitionState(EControllerState from, EControllerState to) noexcept
    {
        State_.compare_exchange_strong(from, to);
        return from;
    }

    void TransitionState(EControllerState from, EControllerState to) noexcept
    {
        auto actual = TryTransitionState(from, to);
        YT_TLOG_FATAL_IF(actual != from, "Unexpected controller state")
            .With("ExpectedState", from)
            .With("ActualState", actual)
            .With("NewState", to);
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
