#include "deferred_publication.h"

#include "client_factory.h"
#include "options.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

#include <ydb-cpp-sdk/client/topic/retry_policy.h>

namespace NYT::NFlow {

using namespace NYdb::NTopic;
using NConcurrency::WaitFor;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TResult>
TResult RunControlOperation(
    std::function<TFuture<TResult>()> operation,
    const TYdbWriteTarget& target,
    const NLogging::TLogger& Logger,
    IStatusErrorStatePtr errorState,
    TStringBuf operationName,
    std::optional<TInstant> deadline)
{
    auto retryState = NYdb::NTopic::IRetryPolicy::GetDefaultPolicy()->CreateRetryState();
    while (true) {
        if (deadline && TInstant::Now() >= *deadline) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Timeout, "Topic preparation attempt timed out")
                .With("endpoint", target.Endpoint)
                .With("database", target.Database);
        }
        auto rawFuture = operation();
        TErrorOr<TResult> result = deadline
            ? WaitFor(rawFuture.ToUncancelable().WithDeadline(*deadline))
            : WaitFor(rawFuture);
        if (!result.IsOK() && rawFuture.IsSet()) {
            result = WaitFor(rawFuture);
        }
        auto value = result.ValueOrThrow();
        if (value.IsSuccess() ||
            value.GetStatus() == NYdb::EStatus::ALREADY_EXISTS ||
            value.GetStatus() == NYdb::EStatus::NOT_FOUND)
        {
            errorState->ClearError();
            return value;
        }
        auto delay = retryState->GetNextRetryDelay(value.GetStatus());
        if (!delay) {
            return value;
        }
        if (deadline && TInstant::Now() + *delay >= *deadline) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Timeout, "Topic preparation attempt timed out")
                .With("endpoint", target.Endpoint)
                .With("database", target.Database);
        }
        errorState->SetError(TError("Transient topic control operation failure")
                .With("operation", operationName)
                .With("endpoint", target.Endpoint)
                .With("database", target.Database)
                .With("ydb_status", value.GetStatus())
                .With("issues", value.GetIssues().ToString()));
        YT_LOG_WARNING("Retrying transient topic control operation (Operation: %v, Status: %v, Backoff: %v)", operationName, value.GetStatus(), *delay);
        WaitFor(NConcurrency::TDelayedExecutor::MakeDelayed(*delay)).ThrowOnError();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TWriterIdentity
    : public NYTree::TYsonStruct
{
    std::string Type;
    int Version{};
    TUniqueSeqNo Generation;
    ui64 PreparationAttempt = 0;
    TSinkId SinkId;
    TPartitionId PartitionId;

    REGISTER_YSON_STRUCT(TWriterIdentity);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("type", &TThis::Type);
        registrar.Parameter("version", &TThis::Version);
        registrar.Parameter("generation", &TThis::Generation);
        registrar.Parameter("preparation_attempt", &TThis::PreparationAttempt);
        registrar.Parameter("sink_id", &TThis::SinkId);
        registrar.Parameter("partition_id", &TThis::PartitionId);
    }
};

DEFINE_REFCOUNTED_TYPE(TWriterIdentity);

std::string MakeDeferredPublicationWriterIdentity(
    const std::string& writerIdentityType,
    TUniqueSeqNo generation,
    ui64 preparationAttempt,
    const TSinkId& sinkId,
    const TPartitionId& partitionId)
{
    auto identity = New<TWriterIdentity>();
    identity->Type = writerIdentityType;
    identity->Version = 2;
    identity->Generation = generation;
    identity->PreparationAttempt = preparationAttempt;
    identity->SinkId = sinkId;
    identity->PartitionId = partitionId;
    auto result = NYson::ConvertToYsonString(identity, NYson::EYsonFormat::Text).ToString();
    if (result.size() > NYdb::NTopic::MaxExtPublicationIdLength) {
        THROW_ERROR_EXCEPTION("Deferred publication writer identity exceeds the SDK size limit")
            .With("actual_length", result.size())
            .With("max_length", NYdb::NTopic::MaxExtPublicationIdLength);
    }
    return result;
}

TDeferredPublicationWriterIdentity ParseDeferredPublicationWriterIdentity(
    const std::string& writerIdentityType,
    const std::string& writerIdentity,
    const TSinkId& sinkId,
    const TPartitionId& partitionId)
{
    TIntrusivePtr<TWriterIdentity> identity;
    try {
        identity = NYTree::ConvertTo<TIntrusivePtr<TWriterIdentity>>(NYson::TYsonStringBuf(writerIdentity));
    } catch (const std::exception&) {
        THROW_ERROR_EXCEPTION("Malformed deferred publication writer identity")
            .With("writer_identity", writerIdentity);
    }
    if (identity->Type != writerIdentityType ||
        identity->Version != 2 ||
        identity->SinkId != sinkId ||
        identity->PartitionId != partitionId)
    {
        THROW_ERROR_EXCEPTION("Foreign deferred publication writer identity")
            .With("writer_identity", writerIdentity);
    }
    return {
        .Generation = identity->Generation,
        .PreparationAttempt = identity->PreparationAttempt,
    };
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct TYdbDeferredPublication::TMetrics
{
    explicit TMetrics(NProfiling::TProfiler profiler)
        : Profiler(std::move(profiler))
    { }

    const NProfiling::TProfiler Profiler;
    NProfiling::TCounter BeginAttempts = Profiler.Counter("/begin/attempts");
    NProfiling::TCounter BeginAlreadyExists = Profiler.WithTag("outcome", "already_exists").Counter("/begin/results");
    NProfiling::TCounter ListAttempts = Profiler.Counter("/list/attempts");
    NProfiling::TEventTimer ListLatency = Profiler.Timer("/list/latency");
    NProfiling::TSummary ListResponseSize = Profiler.Summary("/list/response_size");
    NProfiling::TCounter CancelAttempts = Profiler.Counter("/cancel/attempts");
    NProfiling::TCounter CancelSuccess = Profiler.WithTag("outcome", "success").Counter("/cancel/results");
    NProfiling::TCounter CancelNotFound = Profiler.WithTag("outcome", "not_found").Counter("/cancel/results");
    NProfiling::TCounter CancelFailure = Profiler.WithTag("outcome", "failure").Counter("/cancel/results");
    NProfiling::TCounter Adopt = Profiler.Counter("/begin/adoptions");
    NProfiling::TCounter PublishAttempts = Profiler.Counter("/publish/attempts");
    NProfiling::TCounter PublishSuccess = Profiler.WithTag("outcome", "success").Counter("/publish/results");
    NProfiling::TCounter PublishNotFound = Profiler.WithTag("outcome", "not_found").Counter("/publish/results");
    NProfiling::TCounter PublishFailure = Profiler.WithTag("outcome", "failure").Counter("/publish/results");
    NProfiling::TCounter RecoveryPublishAttempts = Profiler.Counter("/recovery_publish/operations");
    NProfiling::TCounter RecoveryPublishNotFound = Profiler.Counter("/recovery_publish/not_found");
};

struct TYdbDeferredPublication::TPendingPublication
{
    TYdbPendingPublicationSnapshot State;
    TFuture<TPublishResult> PublishFuture;
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> Inflight;
    bool Recovery = false;
};

TYdbDeferredPublication::TYdbDeferredPublication(
    TSinkContextPtr context,
    IYdbClientFactoryPtr clientFactory,
    std::string writerIdentityType,
    TYdbStateAccess<TYdbPublicationSnapshot> stateAccess)
    : Context_(std::move(context))
    , ClientFactory_(std::move(clientFactory))
    , WriterIdentityType_(std::move(writerIdentityType))
    , StateAccess_(std::move(stateAccess))
    , WatermarkPercentile_(Context_->ComputationSpec && Context_->ComputationSpec->WatermarkStrategy
            ? Context_->ComputationSpec->WatermarkStrategy->WatermarkPercentile
            : New<TWatermarkPercentileSpec>())
    , ExternalId_(Format("ytflow/%v/%v", Context_->SinkId, Context_->Partition->PartitionId))
    , Logger(Context_->Logger)
    , ControlErrorState_(Context_->StatusProfiler->ErrorState("/deferred_publication/control"))
    , Metrics_(std::make_unique<TMetrics>(Context_->Profiler.WithPrefix("/deferred_publication")))
{
    YT_VERIFY(ClientFactory_);
}

TYdbDeferredPublication::~TYdbDeferredPublication()
{
    for (const auto& pending : PendingPublications_) {
        if (pending->PublishFuture) {
            pending->PublishFuture.Cancel(TError(NYT::EErrorCode::Canceled, "Deferred publication owner destroyed"));
        }
    }
}

void TYdbDeferredPublication::Init(IInitContextPtr initContext)
{
    StateAccess_.Init(initContext->WithPrefix("deferred_publication"));
    for (const auto& state : StateAccess_.Read().PendingPublications) {
        if (!state.IntPublicationId || !state.Endpoint || state.Endpoint->empty() || !state.Database || state.Database->empty()) {
            THROW_ERROR_EXCEPTION("Pending deferred publication has no persisted destination");
        }
        auto pending = std::make_unique<TPendingPublication>();
        pending->State = state;
        Metrics_->RecoveryPublishAttempts.Increment();
        StartPublish(pending.get(), true);
        PendingPublications_.push_back(std::move(pending));
    }
}

void TYdbDeferredPublication::HarvestFinishedPublishes()
{
    for (int index = std::ssize(PendingPublications_) - 1; index >= 0; --index) {
        if (PendingPublications_[index]->PublishFuture.IsSet()) {
            FinishPublish(index);
        }
    }
}

void TYdbDeferredPublication::WaitForPendingPublications()
{
    while (!PendingPublications_.empty()) {
        FinishPublish(std::ssize(PendingPublications_) - 1);
    }
}

void TYdbDeferredPublication::Prepare(
    const TYdbWriteTarget& target,
    ui64 preparationAttempt,
    std::optional<TInstant> deadline)
{
    if (!PreparedPublication_) {
        YT_VERIFY(!GetBusyTargets().contains(target));
        auto client = ClientFactory_->CreateDeferredPublicationClient(target.Endpoint, target.Database);
        auto publication = OpenPublication(client, target, preparationAttempt, deadline);
        PreparedPublication_.emplace(TPreparedPublication{
            .Target = target,
            .Client = std::move(client),
            .Publication = std::move(publication),
        });
    }
}

void TYdbDeferredPublication::AbandonPreparedPublication()
{
    PreparedPublication_.reset();
    InflightMessages_.clear();
    InflightSnapshot_.clear();
}

const TDeferredPublication& TYdbDeferredPublication::GetPublication() const
{
    YT_VERIFY(PreparedPublication_);
    return PreparedPublication_->Publication;
}

void TYdbDeferredPublication::Sync()
{
    if (PreparedPublication_) {
        SaveState(TYdbPendingPublicationSnapshot{
            .IntPublicationId = PreparedPublication_->Publication.IntPublicationId,
            .Endpoint = PreparedPublication_->Target.Endpoint,
            .Database = PreparedPublication_->Target.Database,
        });
    } else {
        SaveState(std::nullopt);
    }
}

void TYdbDeferredPublication::Commit(bool waitForPublish)
{
    if (PreparedPublication_) {
        auto pending = std::make_unique<TPendingPublication>();
        pending->State = {
            .IntPublicationId = PreparedPublication_->Publication.IntPublicationId,
            .Endpoint = PreparedPublication_->Target.Endpoint,
            .Database = PreparedPublication_->Target.Database,
        };
        pending->Inflight = std::move(InflightSnapshot_);
        StartPublish(pending.get(), false);
        PendingPublications_.push_back(std::move(pending));
        PreparedPublication_.reset();
        InflightMessages_.clear();
        if (waitForPublish) {
            WaitForPendingPublications();
        }
    }
}

void TYdbDeferredPublication::RegisterInflight(const TOutputMessageConstPtr& message)
{
    InflightMessages_.push_back(message);
}

void TYdbDeferredPublication::CompactInflight()
{
    InflightSnapshot_ = BuildBatchInflights(
        Context_->SinkSpec->InputStreamIds,
        InflightMessages_,
        WatermarkPercentile_);
    InflightMessages_.clear();
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TYdbDeferredPublication::BuildInflight() const
{
    auto current = InflightSnapshot_.empty()
        ? BuildBatchInflights(
            Context_->SinkSpec->InputStreamIds,
            InflightMessages_,
            WatermarkPercentile_)
        : InflightSnapshot_;
    THashMap<TStreamId, std::vector<TInflightStreamTraverseDataPtr>> parts;
    for (const auto& [streamId, inflight] : current) {
        parts[streamId].push_back(inflight);
    }
    for (const auto& pending : PendingPublications_) {
        if (pending->Inflight.empty()) {
            for (const auto& streamId : Context_->SinkSpec->InputStreamIds) {
                auto inflight = New<TInflightStreamTraverseData>();
                inflight->Empty = false;
                inflight->Suspended = false;
                inflight->MinSystemTimestamp = ZeroSystemTimestamp;
                inflight->MinEventTimestamp = ZeroSystemTimestamp;
                parts[streamId].push_back(std::move(inflight));
            }
        } else {
            for (const auto& [streamId, inflight] : pending->Inflight) {
                parts[streamId].push_back(inflight);
            }
        }
    }
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> result;
    for (auto& [streamId, inflights] : parts) {
        result.emplace(streamId, MergeInflightTraverseData(inflights, /*allowPartial*/ true));
    }
    return result;
}

THashSet<TYdbWriteTarget, TYdbWriteTargetHash> TYdbDeferredPublication::GetBusyTargets() const
{
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> targets;
    for (const auto& pending : PendingPublications_) {
        targets.insert({.Endpoint = *pending->State.Endpoint, .Database = *pending->State.Database});
    }
    return targets;
}

bool TYdbDeferredPublication::HasPendingPublications() const
{
    return !PendingPublications_.empty();
}

TDeferredPublication TYdbDeferredPublication::OpenPublication(
    IDeferredPublicationClientPtr client,
    const TYdbWriteTarget& target,
    ui64 preparationAttempt,
    std::optional<TInstant> deadline)
{
    const auto generation = Context_->JobGeneration;
    const auto identity = NDetail::MakeDeferredPublicationWriterIdentity(
        WriterIdentityType_,
        generation,
        preparationAttempt,
        Context_->SinkId,
        Context_->Partition->PartitionId);
    while (true) {
        auto begin = RunControlOperation<TBeginPublicationResult>(
            [client, externalId = ExternalId_, identity, beginAttempts = Metrics_->BeginAttempts] {
                beginAttempts.Increment();
                return client->BeginPublication(externalId, identity);
            },
            target,
            Logger,
            ControlErrorState_,
            "Begin",
            deadline);
        if (begin.IsSuccess()) {
            YT_TLOG_INFO("Deferred topic publication opened")
                .With("JobGeneration", generation)
                .With("IntPublicationId", begin.GetIntPublicationId());
            return begin.GetPublication();
        }
        if (begin.GetStatus() != NYdb::EStatus::ALREADY_EXISTS) {
            THROW_ERROR_EXCEPTION("Failed to begin deferred topic publication")
                .With("external_publication_id", ExternalId_)
                .With("ydb_status", begin.GetStatus())
                .With("issues", begin.GetIssues().ToString());
        }
        Metrics_->BeginAlreadyExists.Increment();

        const auto started = TInstant::Now();
        auto list = RunControlOperation<TListPublicationsResult>(
            [client, listAttempts = Metrics_->ListAttempts] {
                listAttempts.Increment();
                return client->ListPublications();
            },
            target,
            Logger,
            ControlErrorState_,
            "List",
            deadline);
        Metrics_->ListLatency.Record(TInstant::Now() - started);
        if (!list.IsSuccess()) {
            THROW_ERROR_EXCEPTION("Failed to list deferred topic publications")
                .With("ydb_status", list.GetStatus())
                .With("issues", list.GetIssues().ToString());
        }
        const auto& publications = list.GetPublications();
        Metrics_->ListResponseSize.Record(std::ssize(publications));
        auto* incumbentInfo = FindIfPtr(publications, [&] (const auto& publication) {
            return publication.ExtPublicationId == ExternalId_;
        });
        if (!incumbentInfo) {
            continue;
        }
        if (!incumbentInfo->WriterIdentity) {
            THROW_ERROR_EXCEPTION("Active deferred topic publication has no writer identity")
                .With("external_publication_id", ExternalId_)
                .With("int_publication_id", incumbentInfo->IntPublicationId);
        }
        const auto owner = NDetail::ParseDeferredPublicationWriterIdentity(
            WriterIdentityType_,
            *incumbentInfo->WriterIdentity,
            Context_->SinkId,
            Context_->Partition->PartitionId);
        TDeferredPublication incumbent(incumbentInfo->IntPublicationId, incumbentInfo->ExtPublicationId);
        if (owner.Generation == generation && owner.PreparationAttempt == preparationAttempt) {
            Metrics_->Adopt.Increment();
            return incumbent;
        }
        if (owner.Generation > generation ||
            (owner.Generation == generation && owner.PreparationAttempt > preparationAttempt))
        {
            THROW_ERROR_EXCEPTION("Stable deferred publication slot is occupied by a newer job")
                .With("external_publication_id", ExternalId_)
                .With("job_generation", generation)
                .With("owner_generation", owner.Generation)
                .With("preparation_attempt", preparationAttempt)
                .With("owner_preparation_attempt", owner.PreparationAttempt);
        }

        // The owner can change after List; cancel only the exact publication we inspected.
        auto cancel = RunControlOperation<TCancelPublicationResult>(
            [
                client,
                intPublicationId = incumbent.IntPublicationId,
                extPublicationId = *incumbent.ExtPublicationId,
                cancelAttempts = Metrics_->CancelAttempts
            ] {
                cancelAttempts.Increment();
                return client->CancelPublication(TDeferredPublication(intPublicationId, extPublicationId));
            },
            target,
            Logger,
            ControlErrorState_,
            "Cancel",
            deadline);
        if (cancel.IsSuccess()) {
            Metrics_->CancelSuccess.Increment();
        } else if (cancel.GetStatus() == NYdb::EStatus::NOT_FOUND) {
            Metrics_->CancelNotFound.Increment();
        } else {
            Metrics_->CancelFailure.Increment();
            THROW_ERROR_EXCEPTION("Failed to cancel orphaned deferred topic publication")
                .With("int_publication_id", incumbent.IntPublicationId)
                .With("ydb_status", cancel.GetStatus())
                .With("issues", cancel.GetIssues().ToString());
        }
        YT_TLOG_INFO("Cancelled orphaned deferred topic publication")
            .With("IntPublicationId", incumbent.IntPublicationId)
            .With("JobGeneration", generation);
    }
}

void TYdbDeferredPublication::StartPublish(TPendingPublication* pending, bool recovery)
{
    const auto publication = TDeferredPublication(*pending->State.IntPublicationId, ExternalId_);
    const TYdbWriteTarget target{
        .Endpoint = *pending->State.Endpoint,
        .Database = *pending->State.Database,
    };
    auto errorState = Context_->StatusProfiler->ErrorState(Format("/deferred_publication/control/targets/%v",
        TYdbWriteTargetHash()(target)));
    pending->Recovery = recovery;
    pending->PublishFuture = BIND([
        clientFactory = ClientFactory_,
        target,
        publication,
        publishAttempts = Metrics_->PublishAttempts,
        Logger = Logger.WithTag("Endpoint", target.Endpoint).WithTag("Database", target.Database),
        errorState = std::move(errorState)
    ] {
        auto client = clientFactory->CreateDeferredPublicationClient(target.Endpoint, target.Database);
        return RunControlOperation<TPublishResult>(
            [client, publication, publishAttempts] {
                publishAttempts.Increment();
                return client->Publish(publication);
            },
            target,
            Logger,
            errorState,
            "Publish",
            std::nullopt);
    }).AsyncVia(Context_->SerializedInvoker)
        .Run();
}

void TYdbDeferredPublication::FinishPublish(int index)
{
    auto& pending = PendingPublications_[index];
    auto result = WaitFor(pending->PublishFuture).ValueOrThrow();
    if (result.IsSuccess()) {
        Metrics_->PublishSuccess.Increment();
    } else if (result.GetStatus() == NYdb::EStatus::NOT_FOUND) {
        Metrics_->PublishNotFound.Increment();
        if (pending->Recovery) {
            Metrics_->RecoveryPublishNotFound.Increment();
        }
    } else {
        Metrics_->PublishFailure.Increment();
        THROW_ERROR_EXCEPTION("Failed to finalize deferred topic publication")
            .With("external_publication_id", ExternalId_)
            .With("int_publication_id", *pending->State.IntPublicationId)
            .With("ydb_status", result.GetStatus())
            .With("issues", result.GetIssues().ToString());
    }
    YT_TLOG_INFO("Finalized deferred topic publication")
        .With("IntPublicationId", *pending->State.IntPublicationId)
        .With("PublishStatus", result.GetStatus());

    PendingPublications_.erase(PendingPublications_.begin() + index);
}

void TYdbDeferredPublication::SaveState(std::optional<TYdbPendingPublicationSnapshot> prepared)
{
    TYdbPublicationSnapshot state;
    state.PendingPublications.reserve(PendingPublications_.size() + (prepared ? 1 : 0));
    for (const auto& pending : PendingPublications_) {
        state.PendingPublications.push_back(pending->State);
    }
    if (prepared) {
        state.PendingPublications.push_back(std::move(*prepared));
    }
    StateAccess_.Write(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
