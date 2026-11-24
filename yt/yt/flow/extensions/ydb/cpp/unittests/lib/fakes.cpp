#include "fakes.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NYdbTesting {

using NYdb::NTopic::TDeferredPublication;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TestLogger("YdbSinkTest");

////////////////////////////////////////////////////////////////////////////////

THoldingInvoker::THoldingInvoker(IInvokerPtr underlyingInvoker)
    : TInvokerWrapper<false>(std::move(underlyingInvoker))
{ }

void THoldingInvoker::Invoke(TClosure callback)
{
    {
        auto guard = Guard(Lock_);
        if (HoldingThread_ == std::this_thread::get_id()) {
            Held_.push_back(std::move(callback));
            return;
        }
    }
    RunBound(std::move(callback));
}

void THoldingInvoker::StartHolding()
{
    auto guard = Guard(Lock_);
    HoldingThread_ = std::this_thread::get_id();
}

void THoldingInvoker::StopHolding()
{
    auto guard = Guard(Lock_);
    HoldingThread_.reset();
}

int THoldingInvoker::GetHeldCount() const
{
    auto guard = Guard(Lock_);
    return std::ssize(Held_);
}

void THoldingInvoker::ReleaseNext()
{
    TClosure callback;
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(!Held_.empty());
        callback = std::move(Held_.front());
        Held_.pop_front();
    }
    RunBound(std::move(callback));
}

void THoldingInvoker::RunBound(TClosure callback)
{
    UnderlyingInvoker_->Invoke(BIND([invoker = IInvokerPtr(this), callback = std::move(callback)] {
        TCurrentInvokerGuard guard(invoker.Get());
        callback();
    }));
}

i64 ReadTaggedCounter(
    const NProfiling::TSolomonRegistryPtr& registry,
    TStringBuf sensorName,
    TStringBuf tagKey,
    TStringBuf tagValue)
{
    const auto dump = registry->DumpSensors();
    i64 result = 0;
    for (const auto& cube : dump.cubes()) {
        if (cube.name() != sensorName) {
            continue;
        }
        for (const auto& projection : cube.projections()) {
            bool tagMatches = false;
            for (auto tagId : projection.tag_ids()) {
                const auto& tag = dump.tags(tagId);
                tagMatches |= tag.key() == tagKey && tag.value() == tagValue;
            }
            if (tagMatches && projection.has_counter()) {
                result = std::max(result, projection.counter());
            }
        }
    }
    return result;
}

i64 ReadCounter(
    const NProfiling::TSolomonRegistryPtr& registry,
    TStringBuf sensorName)
{
    const auto dump = registry->DumpSensors();
    i64 result = 0;
    for (const auto& cube : dump.cubes()) {
        if (cube.name() != sensorName) {
            continue;
        }
        for (const auto& projection : cube.projections()) {
            if (projection.has_counter()) {
                result = std::max(result, projection.counter());
            }
        }
    }
    return result;
}

void CollectProfiler(const NProfiling::TSolomonRegistryPtr& registry)
{
    registry->ProcessRegistrations();
    registry->Collect();
}

////////////////////////////////////////////////////////////////////////////////

void TFakeYdbWriteState::AcknowledgeNext()
{
    YT_VERIFY(!PendingWritePromises.empty());
    PendingWritePromises.front().TrySet();
    PendingWritePromises.erase(PendingWritePromises.begin());
}

void TFakeYdbWriteState::AcknowledgeAll()
{
    for (const auto& promise : PendingWritePromises) {
        promise.TrySet();
    }
    PendingWritePromises.clear();
}

////////////////////////////////////////////////////////////////////////////////

TFakeYdbWriter::TFakeYdbWriter(std::shared_ptr<TFakeYdbWriteState> state)
    : State_(std::move(state))
{ }

TFuture<void> TFakeYdbWriter::Write(TYdbMessageToWrite message)
{
    State_->Writes.push_back(std::move(message));
    if (State_->AutoAcknowledge) {
        if (State_->Writes.size() == State_->NotifyAfterWriteCount) {
            State_->WriteCountReached.TrySet();
        }
        return OKFuture;
    }
    auto promise = NewPromise<void>();
    State_->PendingWritePromises.push_back(promise);
    if (State_->Writes.size() == State_->NotifyAfterWriteCount) {
        State_->WriteCountReached.TrySet();
    }
    if (State_->Writes.size() == State_->SecondNotifyAfterWriteCount) {
        State_->SecondWriteCountReached.TrySet();
    }
    return promise.ToFuture();
}

////////////////////////////////////////////////////////////////////////////////

TControllableRetryableYdbWriter::TControllableRetryableYdbWriter()
    : TRetryableYdbWriter(
        /*topicClient*/ nullptr,
        NYdb::NTopic::TWriteSessionSettings(),
        TestLogger,
        CreateSyncStatusProfiler(TestLogger))
{ }

TFuture<void> TControllableRetryableYdbWriter::Write(TYdbMessageToWrite message)
{
    const auto seqNo = message.SeqNo;
    EmplaceOrCrash(Inflight_, seqNo, message);
    return TRetryableYdbWriter::Write(std::move(message));
}

void TControllableRetryableYdbWriter::ApplyAck(
    i64 seqNo,
    NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState state)
{
    NYdb::NTopic::TWriteSessionEvent::TAcksEvent ackEvent;
    ackEvent.Acks.push_back({
        .SeqNo = static_cast<ui64>(seqNo),
        .State = state,
    });
    ProcessEvent(
        NYdb::NTopic::TWriteSessionEvent::TEvent(std::move(ackEvent)),
        &Token_,
        &Inflight_);
}

void TControllableRetryableYdbWriter::ApplySessionClosed(NYdb::EStatus status)
{
    ProcessEvent(
        NYdb::NTopic::TWriteSessionEvent::TEvent(
            NYdb::NTopic::TSessionClosedEvent(status, {})),
        &Token_,
        &Inflight_);
}

std::vector<i64> TControllableRetryableYdbWriter::GetInflightSeqNos() const
{
    std::vector<i64> result;
    result.reserve(Inflight_.size());
    for (const auto& [seqNo, _] : Inflight_) {
        result.push_back(seqNo);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

NYdb::TStatus MakeStatus(NYdb::EStatus status)
{
    return NYdb::TStatus(status, NYdb::NIssue::TIssues{});
}

NYdb::EStatus PopStatus(std::deque<NYdb::EStatus>& statuses, NYdb::EStatus fallback)
{
    if (statuses.empty()) {
        return fallback;
    }
    auto status = statuses.front();
    statuses.pop_front();
    return status;
}

bool IsTransient(NYdb::EStatus status)
{
    return status == NYdb::EStatus::TIMEOUT || status == NYdb::EStatus::UNAVAILABLE;
}

} // namespace

TFuture<NYdb::NTopic::TBeginPublicationResult> TFakeDeferredPublicationClient::BeginPublication(
    const std::string& externalPublicationId,
    const std::string& writerIdentity)
{
    Actions.push_back("begin");
    BeginCalls.push_back({externalPublicationId, writerIdentity});
    if (PendingBegin) {
        return PendingBegin->ToFuture();
    }
    auto status = PopStatus(BeginStatuses, BeginStatus.load());
    if (status == NYdb::EStatus::SUCCESS && ActivePublications_.contains(externalPublicationId)) {
        status = NYdb::EStatus::ALREADY_EXISTS;
    }
    if (status != NYdb::EStatus::SUCCESS) {
        if (ApplyBeginBeforeTransientStatus && IsTransient(status)) {
            auto id = NextPublicationId++;
            SetActivePublication(id, externalPublicationId, writerIdentity);
        }
        return MakeFuture(NYdb::NTopic::TBeginPublicationResult(MakeStatus(status)));
    }
    auto id = NextPublicationId++;
    SetActivePublication(id, externalPublicationId, writerIdentity);
    return MakeFuture(NYdb::NTopic::TBeginPublicationResult(
        MakeStatus(status),
        TDeferredPublication(id, externalPublicationId)));
}

TFuture<NYdb::NTopic::TListPublicationsResult> TFakeDeferredPublicationClient::ListPublications()
{
    Actions.push_back("list");
    ++ListCallCount;
    if (PendingList) {
        return PendingList->ToFuture();
    }
    std::vector<NYdb::NTopic::TPublicationSummary> publications;
    for (const auto& [_, publication] : ActivePublications_) {
        publications.push_back(publication);
    }
    return MakeFuture(NYdb::NTopic::TListPublicationsResult(
        MakeStatus(PopStatus(ListStatuses, ListStatus)),
        std::move(publications)));
}

TFuture<NYdb::NTopic::TCancelPublicationResult> TFakeDeferredPublicationClient::CancelPublication(
    const TDeferredPublication& publication)
{
    Actions.push_back("cancel");
    CancelCalls.push_back(publication.IntPublicationId);
    if (BeforeCancel) {
        BeforeCancel(*this, publication);
    }
    const bool hasQueuedStatus = !CancelStatuses.empty();
    auto status = hasQueuedStatus
        ? PopStatus(CancelStatuses, NYdb::EStatus::SUCCESS)
        : CancelStatus ? *CancelStatus
                       : Finalize(publication);
    if (status == NYdb::EStatus::SUCCESS && (CancelStatus || hasQueuedStatus)) {
        Finalize(publication);
    } else if (ApplyCancelBeforeTransientStatus && IsTransient(status)) {
        Finalize(publication);
    }
    return MakeFuture(NYdb::NTopic::TCancelPublicationResult(MakeStatus(status)));
}

TFuture<NYdb::NTopic::TPublishResult> TFakeDeferredPublicationClient::Publish(const TDeferredPublication& publication)
{
    Actions.push_back("publish");
    PublishCalls.push_back(publication.IntPublicationId);
    auto status = NYdb::EStatus::SUCCESS;
    if (!PublishStatuses.empty()) {
        status = PublishStatuses.front();
        PublishStatuses.pop_front();
    }
    if (status != NYdb::EStatus::SUCCESS && status != NYdb::EStatus::NOT_FOUND) {
        if (ApplyPublishBeforeTransientStatus && IsTransient(status)) {
            Finalize(publication);
        }
        PublishStarted.TrySet();
        return MakeFuture(NYdb::NTopic::TPublishResult(MakeStatus(status)));
    }
    if (!AutoPublish) {
        auto promise = NewPromise<NYdb::NTopic::TPublishResult>();
        PendingPublishes_.emplace_back(publication, promise);
        LastPendingPublishFuture = promise.ToFuture();
        PublishStarted.TrySet();
        PendingPublishStarted.TrySet();
        return LastPendingPublishFuture;
    }
    PublishStarted.TrySet();
    auto actualStatus = Finalize(publication);
    if (status == NYdb::EStatus::SUCCESS) {
        status = actualStatus;
    }
    return MakeFuture(NYdb::NTopic::TPublishResult(MakeStatus(status)));
}

void TFakeDeferredPublicationClient::CompletePublish(NYdb::EStatus status)
{
    YT_VERIFY(!PendingPublishes_.empty());
    auto [publication, promise] = std::move(PendingPublishes_.front());
    PendingPublishes_.pop_front();
    if (status == NYdb::EStatus::SUCCESS || status == NYdb::EStatus::NOT_FOUND) {
        Finalize(publication);
    }
    promise.Set(NYdb::NTopic::TPublishResult(MakeStatus(status)));
}

void TFakeDeferredPublicationClient::SetActivePublication(
    ui64 intPublicationId,
    const std::string& externalPublicationId,
    std::optional<std::string> writerIdentity)
{
    ActivePublications_[externalPublicationId] = NYdb::NTopic::TPublicationSummary{
        .IntPublicationId = intPublicationId,
        .ExtPublicationId = externalPublicationId,
        .WriterIdentity = std::move(writerIdentity),
    };
}

NYdb::EStatus TFakeDeferredPublicationClient::Finalize(const TDeferredPublication& publication)
{
    auto it = ActivePublications_.find(*publication.ExtPublicationId);
    if (it == ActivePublications_.end() || it->second.IntPublicationId != publication.IntPublicationId) {
        return NYdb::EStatus::NOT_FOUND;
    }
    ActivePublications_.erase(it);
    return NYdb::EStatus::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

TFakeYdbResource::TFakeYdbResource(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext,
    std::shared_ptr<TFakeYdbWriteState> writeState,
    IDeferredPublicationClientPtr publicationClient)
    : TResourceBase(std::move(context), std::move(dynamicContext))
    , WriteState_(std::move(writeState))
    , PublicationClient_(std::move(publicationClient))
{ }

IYdbWriterPtr TFakeYdbResource::CreateWriter(
    const std::string& endpoint,
    const std::string& database,
    NYdb::NTopic::TWriteSessionSettings settings,
    IInvokerPtr invoker,
    NLogging::TLogger /*logger*/,
    IStatusProfilerPtr /*statusProfiler*/)
{
    EXPECT_TRUE(invoker);
    ++WriterCreationCount;
    WriterTargets.emplace_back(endpoint, database);
    WriterSettings.push_back(std::move(settings));
    if (WriterCreationFailures > 0) {
        --WriterCreationFailures;
        THROW_ERROR_EXCEPTION("Failed to create writer");
    }
    return New<TFakeYdbWriter>(WriteState_);
}

IDeferredPublicationClientPtr TFakeYdbResource::CreateDeferredPublicationClient(
    const std::string& endpoint,
    const std::string& database)
{
    ++PublicationClientCreationCount;
    PublicationClientCreated.TrySet();
    PublicationTargets.emplace_back(endpoint, database);
    if (auto it = PublicationClients.find({endpoint, database}); it != PublicationClients.end()) {
        return it->second;
    }
    return PublicationClient_;
}

std::shared_ptr<NYdb::NTopic::TTopicClient> TFakeYdbResource::CreateTopicClient(
    const std::string& /*endpoint*/,
    const std::string& /*database*/)
{
    YT_UNIMPLEMENTED();
}

std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> TFakeYdbResource::CreateFederatedTopicClient(
    const std::string& /*endpoint*/,
    const std::string& /*database*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<TYdbWriteTargetResolution> TFakeYdbResource::ResolveWriteTarget(
    const TYdbTopologyOptions& /*options*/,
    TDuration /*timeout*/,
    std::optional<TYdbWriteTarget> preferredTarget,
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets)
{
    ++ResolutionCallCount;
    ResolutionPreferredTargets.push_back(std::move(preferredTarget));
    ResolutionExcludedTargets.push_back(excludedTargets);
    if (PendingResolution) {
        auto future = PendingResolution->ToFuture();
        ResolutionCalled.TrySet();
        return future;
    }
    if (!Resolutions.empty()) {
        auto result = std::move(Resolutions.front());
        Resolutions.pop_front();
        if (result.Federated && result.Target && excludedTargets.contains(*result.Target)) {
            ResolutionCalled.TrySet();
            return MakeFuture(TYdbWriteTargetResolution{
                .Error = TError("No unoccupied writable cluster"),
                .Retryable = true,
                .Federated = true,
            });
        }
        ResolutionCalled.TrySet();
        return MakeFuture(std::move(result));
    }
    ResolutionCalled.TrySet();
    return MakeFuture(TYdbWriteTargetResolution{
        .Target = TYdbWriteTarget{.Endpoint = "localhost:2135", .Database = "/Root"},
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
