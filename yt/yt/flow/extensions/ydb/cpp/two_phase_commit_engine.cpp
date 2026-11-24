#include "two_phase_commit_engine.h"

#include "sink_engine.h"

#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::vector<TYdbWriteTarget> NDetail::ExtractExpiredYdbTargets(
    TYdbTargetCooldownMap* retryAfterByTarget,
    TInstant now)
{
    std::vector<TYdbWriteTarget> result;
    for (auto it = retryAfterByTarget->begin(); it != retryAfterByTarget->end();) {
        if (it->second <= now) {
            result.push_back(it->first);
            auto expired = it++;
            retryAfterByTarget->erase(expired);
        } else {
            ++it;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TYdbTwoPhaseCommitEngine::TMetrics
{
    explicit TMetrics(NProfiling::TProfiler profiler)
        : Attempts(profiler.Counter("/deferred_publication/preparation/attempts"))
        , Timeouts(profiler.Counter("/deferred_publication/preparation/timeouts"))
        , CoolingTargetCount(profiler.Gauge("/deferred_publication/preparation/cooling_target_count"))
    { }

    NProfiling::TCounter Attempts;
    NProfiling::TCounter Timeouts;
    NProfiling::TGauge CoolingTargetCount;
};

////////////////////////////////////////////////////////////////////////////////

TYdbTwoPhaseCommitEngine::TYdbTwoPhaseCommitEngine(
    TSinkContextPtr context,
    std::shared_ptr<const TYdbTwoPhaseCommitOptions> options,
    std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> dynamicOptions,
    TYdbSinkEnginePtr writer,
    TYdbDeferredPublicationPtr publication,
    TYdbStateAccess<TYdbSinkCheckpoint> checkpointState)
    : Context_(std::move(context))
    , Options_(std::move(options))
    , DynamicOptions_(std::move(dynamicOptions))
    , Writer_(std::move(writer))
    , Publication_(std::move(publication))
    , CheckpointState_(std::move(checkpointState))
    , Logger(Writer_->GetLogger())
    , Federated_(Options_->Sink.Topology.KnownFederated)
    , TargetErrorState_(Context_->StatusProfiler->ErrorState("/write_target_discovery"))
    , Metrics_(std::make_unique<TMetrics>(Context_->Profiler))
{ }

TYdbTwoPhaseCommitEngine::~TYdbTwoPhaseCommitEngine()
{
    if (TargetFuture_) {
        TargetFuture_.Cancel(TError(NYT::EErrorCode::Canceled, "Topic sink destroyed"));
    }
}

void TYdbTwoPhaseCommitEngine::ValidateInput(const std::string& sinkTypeName) const
{
    if (Context_->SinkSpec->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Sink %Qv expects exactly one input stream but got %v",
            sinkTypeName,
            Context_->SinkSpec->InputStreamIds.size());
    }
    const auto& streamId = *Context_->SinkSpec->InputStreamIds.begin();
    auto schema = Context_->StreamSpecStorage->GetSchema(streamId);
    if (!schema->FindColumn(Options_->Sink.PayloadColumn)) {
        THROW_ERROR_EXCEPTION("Stream %Qv should have %Qv column to be distributed by %v",
            streamId,
            Options_->Sink.PayloadColumn,
            sinkTypeName);
    }
}

void TYdbTwoPhaseCommitEngine::Init(IInitContextPtr initContext)
{
    CheckpointState_.Init(initContext);
    auto checkpoint = CheckpointState_.Read();
    if (checkpoint.ProducerId.empty()) {
        checkpoint.ProducerId = ToString(TGuid::Create());
        CheckpointState_.Write(checkpoint);
    }
    LastMessageId_ = checkpoint.MaxPersistedMessageId;
    LastSeqNo_ = checkpoint.MaxPersistedSeqNo;
    Publication_->Init(initContext);
    StartTargetResolution();
}

void TYdbTwoPhaseCommitEngine::Reconfigure(std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> dynamicOptions)
{
    DynamicOptions_ = std::move(dynamicOptions);
}

void TYdbTwoPhaseCommitEngine::StartTargetResolution()
{
    ExpireTargetCooldowns();
    TargetResolutionStartedAt_ = TInstant::Now();
    TargetFuture_ = BIND([
        clientFactory = Writer_->GetClientFactory(),
        options = Options_,
        preferredTarget = Target_,
        excludedTargets = GetExcludedTargets()
    ] {
        return clientFactory->ResolveWriteTarget(
            options->Sink.Topology,
            options->Sink.Topology.ClientTimeout,
            preferredTarget,
            excludedTargets);
    }).AsyncVia(Context_->SerializedInvoker)
        .Run();
}

void TYdbTwoPhaseCommitEngine::ExpireTargetCooldowns()
{
    for (const auto& target : NDetail::ExtractExpiredYdbTargets(&RetryAfterByTarget_, TInstant::Now())) {
        if (auto errorState = PreparationErrorStates_.find(target); errorState != PreparationErrorStates_.end()) {
            errorState->second->ClearError();
        }
    }
    Metrics_->CoolingTargetCount.Update(RetryAfterByTarget_.size());
}

THashSet<TYdbWriteTarget, TYdbWriteTargetHash> TYdbTwoPhaseCommitEngine::GetExcludedTargets() const
{
    auto result = Publication_->GetBusyTargets();
    for (const auto& [target, retryAfter] : RetryAfterByTarget_) {
        Y_UNUSED(retryAfter);
        result.insert(target);
    }
    return result;
}

void TYdbTwoPhaseCommitEngine::EnsureWriteTarget(bool initializeSession)
{
    Publication_->HarvestFinishedPublishes();
    ExpireTargetCooldowns();
    const auto now = TInstant::Now();
    if (Target_ &&
        !GetExcludedTargets().contains(*Target_) &&
        LastTargetRefreshAt_ &&
        now < *LastTargetRefreshAt_ + Options_->Sink.Topology.UpdatePartitionCountPeriod)
    {
        return;
    }
    if (!TargetFuture_ || now >= TargetResolutionStartedAt_ + Options_->Sink.Topology.UpdatePartitionCountPeriod) {
        if (TargetFuture_) {
            TargetFuture_.Cancel(TError(NYT::EErrorCode::Canceled, "Superseded stale topic target resolution"));
        }
        StartTargetResolution();
    }

    TExponentialBackoffOptions backoffOptions;
    backoffOptions.InvocationCount = std::numeric_limits<decltype(backoffOptions.InvocationCount)>::max();
    backoffOptions.MaxBackoff = TDuration::Seconds(30);
    TBackoffStrategy backoffStrategy(backoffOptions);
    while (true) {
        const auto resolutionStartedAt = TargetResolutionStartedAt_;
        auto resolution = NConcurrency::WaitFor(TargetFuture_).ValueOrThrow();
        TargetFuture_.Reset();
        if (resolution.Target) {
            if (Federated_ && *Federated_ != resolution.Federated) {
                THROW_ERROR_EXCEPTION("Topic discovery semantics changed during the sink lifetime");
            }
            Federated_ = resolution.Federated;
            if (!resolution.Federated) {
                Publication_->WaitForPendingPublications();
            }
            if (!initializeSession) {
                TargetErrorState_->ClearError();
                return;
            }
            const bool targetChanged = Target_ != resolution.Target;
            if (targetChanged) {
                Writer_->InitSession(CheckpointState_.Read().ProducerId, *resolution.Target);
            }
            Target_ = std::move(resolution.Target);
            LastTargetRefreshAt_ = resolutionStartedAt;
            TargetErrorState_->ClearError();
            return;
        }
        if (resolution.Error.IsOK()) {
            THROW_ERROR_EXCEPTION("Topic target resolution returned neither target nor error");
        }
        TargetErrorState_->SetError(resolution.Error);
        if (!resolution.Retryable) {
            resolution.Error.ThrowOnError();
        }
        backoffStrategy.Next();
        NConcurrency::TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
        Publication_->HarvestFinishedPublishes();
        StartTargetResolution();
    }
}

void TYdbTwoPhaseCommitEngine::Distribute(
    const TOutputMessageConstPtr& message,
    TOnDistributedCallback onDistributed)
{
    // A different destination may have kept this message in the core output store.
    if (CheckpointState_.IsPersisted(message->MessageId)) {
        onDistributed();
        return;
    }
    if (message->MessageId <= LastMessageId_) {
        THROW_ERROR_EXCEPTION("Expected message id to be greater than the last distributed message id")
            .With("message_id", message->MessageId)
            .With("last_distributed_message_id", LastMessageId_);
    }
    LastMessageId_ = message->MessageId;
    Requests_.push_back({message, std::move(onDistributed)});
}

void TYdbTwoPhaseCommitEngine::WritePublicationMessages(std::optional<TInstant> deadline, const TYdbBatchingOptions& batching)
{
    YT_VERIFY(Target_);
    std::vector<TFuture<void>> futures;
    std::vector<TOutputMessageConstPtr> batch;
    auto limiter = TMessageBatchLimiter(batching.MaxRowsPerBatch, batching.MaxBytesPerBatch);
    auto flush = [&] {
        if (batch.empty()) {
            return;
        }
        futures.push_back(Writer_->WriteFramingBatch(
            ++LastSeqNo_,
            batch,
            Publication_->GetPublication()));
        batch.clear();
        limiter = TMessageBatchLimiter(batching.MaxRowsPerBatch, batching.MaxBytesPerBatch);
    };
    for (const auto& request : Requests_) {
        const auto& message = request.Message;
        Publication_->RegisterInflight(message);
        if (Options_->Sink.FramingFormat) {
            batch.push_back(message);
            limiter.Add(message->ByteSize);
            if (limiter.IsFull()) {
                flush();
            }
        } else {
            futures.push_back(Writer_->WriteMessage(
                ++LastSeqNo_,
                *message,
                Publication_->GetPublication()));
        }
    }
    flush();
    auto writeFuture = AllSucceeded(std::move(futures));
    TError error;
    if (deadline) {
        error = NConcurrency::WaitFor(writeFuture.ToUncancelable().WithDeadline(*deadline));
        if (!error.IsOK() && writeFuture.IsSet()) {
            error = NConcurrency::WaitFor(writeFuture);
        }
    } else {
        error = NConcurrency::WaitFor(writeFuture);
    }
    error.ThrowOnError();
}

void TYdbTwoPhaseCommitEngine::PreSyncFlush(TYdbEventLagObserver observeEventLag)
{
    ExpireTargetCooldowns();
    Publication_->HarvestFinishedPublishes();
    if (Requests_.empty() &&
        Publication_->HasPendingPublications() &&
        (!Federated_.has_value() || !*Federated_))
    {
        if (!Federated_.has_value()) {
            EnsureWriteTarget(/*initializeSession*/ false);
        }
        if (!*Federated_) {
            Publication_->WaitForPendingPublications();
        }
    }
    while (!Requests_.empty()) {
        EnsureWriteTarget();
        ++PreparationAttemptSerial_;
        Metrics_->Attempts.Increment();
        const auto startedAt = TInstant::Now();
        const auto dynamicOptions = DynamicOptions_;
        const auto deadline = Federated_.value_or(false)
            ? std::optional(startedAt + dynamicOptions->PreparationAttemptTimeout)
            : std::nullopt;
        const auto targetBackoff = dynamicOptions->PreparationTargetBackoff;
        TStringBuf phase = "control";
        try {
            Publication_->Prepare(*Target_, PreparationAttemptSerial_, deadline);
            phase = "write_ack";
            WritePublicationMessages(deadline, dynamicOptions->Batching);
        } catch (const std::exception& ex) {
            if (!Federated_.value_or(false) || !TError(ex).FindMatching(NYT::EErrorCode::Timeout)) {
                throw;
            }
            RetirePreparationAttempt(targetBackoff, startedAt, phase);
            continue;
        }
        if (auto it = PreparationErrorStates_.find(*Target_); it != PreparationErrorStates_.end()) {
            it->second->ClearError();
        }
        YT_TLOG_INFO("Prepared topic publication attempt")
            .With("Endpoint", Target_->Endpoint)
            .With("Database", Target_->Database)
            .With("PreparationAttempt", PreparationAttemptSerial_);
        break;
    }
    if (!Requests_.empty()) {
        Publication_->CompactInflight();
        auto checkpoint = CheckpointState_.Read();
        checkpoint.MaxPersistedMessageId = Requests_.back().Message->MessageId;
        checkpoint.MaxPersistedSeqNo = LastSeqNo_;
        CheckpointState_.Write(checkpoint);
        for (auto& request : Requests_) {
            observeEventLag(request.Message->StreamId, request.Message->EventTimestamp);
            request.Callback();
        }
        Requests_.clear();
    }
}

void TYdbTwoPhaseCommitEngine::RetirePreparationAttempt(
    TDuration targetBackoff,
    TInstant startedAt,
    TStringBuf phase)
{
    YT_VERIFY(Target_);
    Metrics_->Timeouts.Increment();
    auto [it, inserted] = PreparationErrorStates_.emplace(
        *Target_,
        Context_->StatusProfiler->ErrorState(Format("/deferred_publication/preparation/targets/%v",
        TYdbWriteTargetHash()(*Target_))));
    Y_UNUSED(inserted);
    it->second->SetError(TError("Topic preparation attempt timed out")
            .With("endpoint", Target_->Endpoint)
            .With("database", Target_->Database)
            .With("preparation_attempt", PreparationAttemptSerial_));
    YT_TLOG_WARNING("Retiring timed out topic preparation attempt")
        .With("Endpoint", Target_->Endpoint)
        .With("Database", Target_->Database)
        .With("TargetHash", TYdbWriteTargetHash()(*Target_))
        .With("JobGeneration", Context_->JobGeneration)
        .With("PreparationAttempt", PreparationAttemptSerial_)
        .With("Phase", phase)
        .With("Elapsed", TInstant::Now() - startedAt)
        .With("Cooldown", targetBackoff);
    RetryAfterByTarget_[*Target_] = TInstant::Now() + targetBackoff;
    Metrics_->CoolingTargetCount.Update(RetryAfterByTarget_.size());
    Writer_->ResetSession();
    Publication_->AbandonPreparedPublication();
    Target_.reset();
    LastTargetRefreshAt_.reset();
    StartTargetResolution();
}

void TYdbTwoPhaseCommitEngine::Sync()
{
    Publication_->Sync();
}

void TYdbTwoPhaseCommitEngine::Commit()
{
    Publication_->Commit(!Federated_.value_or(false));
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TYdbTwoPhaseCommitEngine::BuildInflight() const
{
    return Publication_->BuildInflight();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
