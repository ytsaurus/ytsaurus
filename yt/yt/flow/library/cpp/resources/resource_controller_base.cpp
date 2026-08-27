#include "resource_controller_base.h"

#include <yt/yt/flow/library/cpp/common/file_source.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TFileSourceDiscoveryState
    : public TYsonStruct
{
    IMapNodePtr FileSources;
    IMapNodePtr DynamicFileSources;
    THashMap<TFileSourceId, TFileSourceRevisionPtr> Revisions;
    std::optional<TFileSnapshotId> ActiveFileSnapshotId;
    std::optional<TFileSnapshotId> PreparingFileSnapshotId;
    THashMap<TFileSnapshotId, TFileSnapshotPtr> KnownFileSnapshots;
    std::optional<TInstant> LastFileSnapshotCreationTime;
    std::optional<TInstant> ActiveFileSnapshotPublishedAt;

    REGISTER_YSON_STRUCT(TFileSourceDiscoveryState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("file_sources", &TThis::FileSources)
            .Default();
        registrar.Parameter("dynamic_file_sources", &TThis::DynamicFileSources)
            .Default();
        registrar.Parameter("revisions", &TThis::Revisions)
            .Default();
        registrar.Parameter("active_file_snapshot_id", &TThis::ActiveFileSnapshotId)
            .Default();
        registrar.Parameter("preparing_file_snapshot_id", &TThis::PreparingFileSnapshotId)
            .Default();
        registrar.Parameter("known_file_snapshots", &TThis::KnownFileSnapshots)
            .Default();
        registrar.Parameter("last_file_snapshot_creation_time", &TThis::LastFileSnapshotCreationTime)
            .Default();
        registrar.Parameter("active_file_snapshot_published_at", &TThis::ActiveFileSnapshotPublishedAt)
            .Default();
    }
};

struct TFileSourceDiscoveryView
    : public TYsonStruct
{
    THashMap<TFileSourceId, TFileSourceRevisionPtr> SourceRevisions;
    std::optional<TFileSnapshotId> ActiveFileSnapshotId;
    std::optional<TFileSnapshotId> PreparingFileSnapshotId;
    i64 KnownFileSnapshotCount = 0;
    i64 UnknownFileSnapshotCount = 0;
    std::optional<TInstant> ActiveFileSnapshotPublishedAt;
    std::optional<TDuration> FileSnapshotRolloutAge;
    i64 RolloutInstanceCount = 0;
    i64 RolloutConvergedInstanceCount = 0;
    i64 RolloutLaggingInstanceCount = 0;
    i64 RolloutUninitializedInstanceCount = 0;
    i64 RolloutBlockingAccessorCount = 0;
    THashMap<std::string, i64> RolloutProgressStateCounts;
    THashMap<std::string, TError> RolloutErrors;
    THashMap<std::string, i64> FileSnapshotStateCounts;
    THashMap<std::string, i64> FileSourceRevisionStateCounts;
    THashMap<TFileSnapshotId, i64> LiveAccessorCounts;

    REGISTER_YSON_STRUCT(TFileSourceDiscoveryView);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("source_revisions", &TThis::SourceRevisions)
            .Default();
        registrar.Parameter("active_file_snapshot_id", &TThis::ActiveFileSnapshotId)
            .Default();
        registrar.Parameter("preparing_file_snapshot_id", &TThis::PreparingFileSnapshotId)
            .Default();
        registrar.Parameter("known_file_snapshot_count", &TThis::KnownFileSnapshotCount)
            .Default();
        registrar.Parameter("unknown_file_snapshot_count", &TThis::UnknownFileSnapshotCount)
            .Default();
        registrar.Parameter("active_file_snapshot_published_at", &TThis::ActiveFileSnapshotPublishedAt)
            .Default();
        registrar.Parameter("file_snapshot_rollout_age", &TThis::FileSnapshotRolloutAge)
            .Default();
        registrar.Parameter("rollout_instance_count", &TThis::RolloutInstanceCount)
            .Default();
        registrar.Parameter("rollout_converged_instance_count", &TThis::RolloutConvergedInstanceCount)
            .Default();
        registrar.Parameter("rollout_lagging_instance_count", &TThis::RolloutLaggingInstanceCount)
            .Default();
        registrar.Parameter("rollout_uninitialized_instance_count", &TThis::RolloutUninitializedInstanceCount)
            .Default();
        registrar.Parameter("rollout_blocking_accessor_count", &TThis::RolloutBlockingAccessorCount)
            .Default();
        registrar.Parameter("rollout_progress_state_counts", &TThis::RolloutProgressStateCounts)
            .Default();
        registrar.Parameter("rollout_errors", &TThis::RolloutErrors)
            .Default();
        registrar.Parameter("file_snapshot_state_counts", &TThis::FileSnapshotStateCounts)
            .Default();
        registrar.Parameter("file_source_revision_state_counts", &TThis::FileSourceRevisionStateCounts)
            .Default();
        registrar.Parameter("live_accessor_counts", &TThis::LiveAccessorCounts)
            .Default();
    }
};

TDynamicFileSourceSpecPtr GetDynamicFileSourceSpec(
    const TDynamicResourceSpecPtr& dynamicResourceSpec,
    const TFileSourceId& id)
{
    if (auto it = dynamicResourceSpec->FileSources.find(id);
        it != dynamicResourceSpec->FileSources.end())
    {
        return it->second;
    }

    auto result = New<TDynamicFileSourceSpec>();
    result->Parameters = GetEphemeralNodeFactory()->CreateMap();
    return result;
}

THashMap<TFileSourceId, TDynamicFileSourceSpecPtr> BuildDynamicFileSourceSpecs(
    const TResourceSpecPtr& resourceSpec,
    const TDynamicResourceSpecPtr& dynamicResourceSpec)
{
    THashMap<TFileSourceId, TDynamicFileSourceSpecPtr> result;
    for (const auto& [id, _] : resourceSpec->FileSources) {
        EmplaceOrCrash(result, id, GetDynamicFileSourceSpec(dynamicResourceSpec, id));
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TResourceControllerBase::TFileSourceDiscovery
    : public TRefCounted
{
public:
    TFileSourceDiscovery(
        TResourceControllerContextPtr context,
        const TDynamicResourceControllerContextPtr& dynamicContext)
        : Context_(std::move(context))
        , FileSnapshotMinCreationPeriod_(dynamicContext->DynamicResourceSpec->FileSnapshotMinCreationPeriod)
        , FileSnapshotCatalogMaxEntries_(dynamicContext->DynamicResourceSpec->FileSnapshotCatalogMaxEntries)
        , FileSnapshotRolloutWarningPeriod_(dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod)
    {
        if (!Context_->ResourceSpec->FileSources.empty()) {
            UnknownFileSnapshotCountGauge_ = Context_->Profiler.Gauge("/unknown_file_snapshot_count");
            RolloutWarningErrorState_ = Context_->StatusProfiler->ErrorState("/file_snapshot_rollout");
        }

        auto dynamicFileSources = BuildDynamicFileSourceSpecs(
            Context_->ResourceSpec,
            dynamicContext->DynamicResourceSpec);
        DynamicFileSources_ = ConvertToNode(dynamicFileSources)->AsMap();

        for (const auto& [id, spec] : Context_->ResourceSpec->FileSources) {
            auto sourceContext = New<TFileSourceContext>();
            sourceContext->SourceSpec = spec;
            sourceContext->ClientsCache = Context_->ClientsCache;
            sourceContext->PipelinePath = Context_->PipelinePath;
            sourceContext->Invoker = Context_->Invoker;
            sourceContext->Logger = Context_->Logger
                .WithTag("Component", "FileSource")
                .WithTag("FileSource", id);

            auto errorState = Context_->StatusProfiler->ErrorState(
                Format("/file_sources/%v/discovery", id));
            auto dynamicSourceContext = New<TDynamicFileSourceContext>();
            dynamicSourceContext->DynamicFileSourceSpec = GetOrCrash(dynamicFileSources, id);
            auto executor = New<TPeriodicExecutor>(
                Context_->Invoker,
                BIND(&TFileSourceDiscovery::Discover, MakeWeak(this), id),
                dynamicContext->DynamicResourceSpec->FileSourceDiscoverPeriod);
            TSourceEntry sourceEntry{
                .Spec = spec,
                .DynamicSpec = dynamicSourceContext->DynamicFileSourceSpec,
                .Source = TRegistry::Get()->CreateFileSource(sourceContext, dynamicSourceContext),
                .DiscoveryError = std::move(errorState),
                .DiscoveryExecutor = std::move(executor),
            };
            EmplaceOrCrash(Sources_, id, std::move(sourceEntry));
        }
    }

    void Init(const IInitContextPtr& initContext)
    {
        if (initContext) {
            initContext->InitClient<TFileSourceDiscoveryState>(State_, "v0");

            auto fileSources = ConvertToNode(Context_->ResourceSpec->FileSources)->AsMap();
            if (State_->FileSources &&
                State_->DynamicFileSources &&
                AreNodesEqual(State_->FileSources, fileSources) &&
                AreNodesEqual(State_->DynamicFileSources, DynamicFileSources_))
            {
                auto guard = Guard(Lock_);
                for (const auto& [id, revision] : State_->Revisions) {
                    auto sourceIt = Sources_.find(id);
                    if (sourceIt != Sources_.end() &&
                        revision &&
                        revision->FileSourceClassName == sourceIt->second.Spec->FileSourceClassName)
                    {
                        PendingRevisions_[id] = revision;
                    }
                }
                if (PendingRevisions_.size() == Sources_.size()) {
                    PublishedRevisions_ = PendingRevisions_;
                }
            } else {
                State_->FileSources = std::move(fileSources);
                State_->DynamicFileSources = DynamicFileSources_;
                State_->Revisions.clear();
            }

            {
                auto guard = Guard(Lock_);
                KnownFileSnapshots_ = State_->KnownFileSnapshots;
                auto activeFileSnapshot = FindKnownFileSnapshot(State_->ActiveFileSnapshotId);
                auto preparingFileSnapshot = FindKnownFileSnapshot(State_->PreparingFileSnapshotId);
                if (ArePersistedFileSnapshotsCompatible(activeFileSnapshot, preparingFileSnapshot)) {
                    ActiveFileSnapshot_ = std::move(activeFileSnapshot);
                    PreparingFileSnapshot_ = std::move(preparingFileSnapshot);
                    LastFileSnapshotCreationTime_ = State_->LastFileSnapshotCreationTime;
                    ActiveFileSnapshotPublishedAt_ = State_->ActiveFileSnapshotPublishedAt;
                    if (ActiveFileSnapshot_ && !ActiveFileSnapshotPublishedAt_) {
                        ActiveFileSnapshotPublishedAt_ = TInstant::Now();
                    }
                } else {
                    ActiveFileSnapshot_.Reset();
                    PreparingFileSnapshot_.Reset();
                    LastFileSnapshotCreationTime_.reset();
                    ActiveFileSnapshotPublishedAt_.reset();
                }
                PruneKnownFileSnapshots();
                PersistFileSnapshotState();
            }
        }

        for (const auto& [_, entry] : Sources_) {
            entry.DiscoveryExecutor->Start();
        }
    }

    void Reconfigure(const TDynamicResourceControllerContextPtr& dynamicContext)
    {
        auto dynamicFileSources = BuildDynamicFileSourceSpecs(
            Context_->ResourceSpec,
            dynamicContext->DynamicResourceSpec);
        auto dynamicFileSourcesNode = ConvertToNode(dynamicFileSources)->AsMap();
        std::vector<TPeriodicExecutorPtr> changedExecutors;

        for (auto& [id, entry] : Sources_) {
            entry.DiscoveryExecutor->SetPeriod(
                dynamicContext->DynamicResourceSpec->FileSourceDiscoverPeriod);

            const auto& dynamicSpec = GetOrCrash(dynamicFileSources, id);
            if (AreNodesEqual(ConvertToNode(entry.DynamicSpec), ConvertToNode(dynamicSpec))) {
                continue;
            }

            auto dynamicSourceContext = New<TDynamicFileSourceContext>();
            dynamicSourceContext->DynamicFileSourceSpec = dynamicSpec;
            entry.Source->Reconfigure(dynamicSourceContext);
            {
                auto guard = Guard(Lock_);
                entry.DynamicSpec = dynamicSpec;
                ++entry.Generation;
                PendingRevisions_.erase(id);
            }
            changedExecutors.push_back(entry.DiscoveryExecutor);
        }

        {
            auto guard = Guard(Lock_);
            FileSnapshotMinCreationPeriod_ = dynamicContext->DynamicResourceSpec->FileSnapshotMinCreationPeriod;
            FileSnapshotCatalogMaxEntries_ = dynamicContext->DynamicResourceSpec->FileSnapshotCatalogMaxEntries;
            FileSnapshotRolloutWarningPeriod_ = dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod;
            PruneKnownFileSnapshots();
            if (!AreNodesEqual(DynamicFileSources_, dynamicFileSourcesNode)) {
                DynamicFileSources_ = std::move(dynamicFileSourcesNode);
                if (State_.IsInitialized()) {
                    State_->DynamicFileSources = DynamicFileSources_;
                    State_->Revisions = PendingRevisions_;
                }
            }
            PersistFileSnapshotState();
        }

        for (const auto& executor : changedExecutors) {
            executor->ScheduleOutOfBand();
        }
    }

    std::optional<std::pair<TFileSnapshotPtr, TFileSnapshotPtr>> BuildTargetFileSnapshots()
    {
        std::optional<THashMap<TFileSourceId, TFileSourceRevisionPtr>> revisionsToSnapshot;
        {
            auto guard = Guard(Lock_);
            auto now = TInstant::Now();
            if (LastFileSnapshotCreationTime_ && *LastFileSnapshotCreationTime_ > now) {
                LastFileSnapshotCreationTime_ = now;
                PersistFileSnapshotState();
            }
            if (PublishedRevisions_.size() == Sources_.size() &&
                MatchesFileSnapshot(ActiveFileSnapshot_, PublishedRevisions_) &&
                PreparingFileSnapshot_)
            {
                PreparingFileSnapshot_.Reset();
                PersistFileSnapshotState();
            }
            if (!Sources_.empty() &&
                PublishedRevisions_.size() == Sources_.size() &&
                !MatchesFileSnapshot(ActiveFileSnapshot_, PublishedRevisions_) &&
                !MatchesFileSnapshot(PreparingFileSnapshot_, PublishedRevisions_) &&
                (!LastFileSnapshotCreationTime_ ||
                    now >= *LastFileSnapshotCreationTime_ + FileSnapshotMinCreationPeriod_))
            {
                revisionsToSnapshot = PublishedRevisions_;
            }
        }

        if (revisionsToSnapshot) {
            THROW_ERROR_EXCEPTION_UNLESS(
                Context_->TimeProvider,
                "File source controller requires a time provider");
            auto snapshot = New<TFileSnapshot>();
            snapshot->Id = TFileSnapshotId(Context_->TimeProvider->GenerateSeqNo());
            snapshot->FileSources = *revisionsToSnapshot;

            auto guard = Guard(Lock_);
            auto now = TInstant::Now();
            if (AreFileSourceRevisionsEqual(PublishedRevisions_, *revisionsToSnapshot) &&
                !MatchesFileSnapshot(ActiveFileSnapshot_, *revisionsToSnapshot) &&
                !MatchesFileSnapshot(PreparingFileSnapshot_, *revisionsToSnapshot) &&
                (!LastFileSnapshotCreationTime_ ||
                    now >= *LastFileSnapshotCreationTime_ + FileSnapshotMinCreationPeriod_))
            {
                PreparingFileSnapshot_ = std::move(snapshot);
                RegisterKnownFileSnapshot(PreparingFileSnapshot_);
                LastFileSnapshotCreationTime_ = now;
                PersistFileSnapshotState();
            }
        }

        auto guard = Guard(Lock_);
        if (!Sources_.empty() && !ActiveFileSnapshot_ && !PreparingFileSnapshot_) {
            return std::nullopt;
        }
        return std::pair(ActiveFileSnapshot_, PreparingFileSnapshot_);
    }

    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        std::optional<i64> publishedRevisionId)
    {
        if (Sources_.empty()) {
            return;
        }

        auto authoritativeWorkerStatuses = FilterAuthoritativeStatuses(workerStatuses);
        THashMap<std::string, TWorkerResourceStatusPtr> currentTargetWorkerStatuses;
        if (publishedRevisionId) {
            for (const auto& [workerAddress, status] : authoritativeWorkerStatuses) {
                if (status->TargetRevisionId == publishedRevisionId) {
                    currentTargetWorkerStatuses.emplace(workerAddress, status);
                }
            }
        }

        THashMap<TFileSnapshotId, TFileSnapshotPtr> knownFileSnapshots;
        {
            auto guard = Guard(Lock_);
            knownFileSnapshots = KnownFileSnapshots_;
        }

        THashMap<std::pair<TFileSnapshotId, EFileSnapshotState>, i64> fileSnapshotStateCounts;
        THashMap<std::tuple<TFileSourceId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, i64> fileSourceRevisionStateCounts;
        THashMap<TFileSnapshotId, i64> liveAccessorCounts;
        i64 unknownFileSnapshotCount = 0;
        for (const auto& [_, status] : authoritativeWorkerStatuses) {
            THashMap<TFileSnapshotId, EFileSnapshotState> workerFileSnapshotStates;
            THashMap<std::pair<TFileSourceId, NFileStorage::TFileStorageObjectId>, EFileSnapshotState> workerFileSourceRevisionStates;
            auto accountFileSnapshot = [&] (
                TFileSnapshotId snapshotId,
                EFileSnapshotState state) {
                auto [it, inserted] = workerFileSnapshotStates.emplace(snapshotId, state);
                if (!inserted && state > it->second) {
                    it->second = state;
                }
            };
            if (status->ActiveFileSnapshotId) {
                accountFileSnapshot(
                    *status->ActiveFileSnapshotId,
                    EFileSnapshotState::Active);
            }
            if (status->PreparingFileSnapshot) {
                accountFileSnapshot(
                    status->PreparingFileSnapshot->SnapshotId,
                    status->PreparingFileSnapshot->State);
            }

            for (const auto& [snapshotId, state] : workerFileSnapshotStates) {
                ++fileSnapshotStateCounts[std::pair(snapshotId, state)];
                auto snapshotIt = knownFileSnapshots.find(snapshotId);
                if (snapshotIt == knownFileSnapshots.end() || !snapshotIt->second) {
                    ++unknownFileSnapshotCount;
                    continue;
                }
                for (const auto& [fileSourceId, revision] : snapshotIt->second->FileSources) {
                    if (!revision) {
                        continue;
                    }
                    auto key = std::pair(fileSourceId, revision->ObjectId);
                    auto [revisionIt, revisionInserted] = workerFileSourceRevisionStates.emplace(key, state);
                    if (!revisionInserted && state > revisionIt->second) {
                        revisionIt->second = state;
                    }
                }
            }
            for (const auto& [key, state] : workerFileSourceRevisionStates) {
                ++fileSourceRevisionStateCounts[std::tuple(key.first, key.second, state)];
            }
            for (const auto& [snapshotId, count] : status->LiveAccessorCounts) {
                if (count > 0) {
                    liveAccessorCounts[snapshotId] += count;
                }
            }
        }

        if (publishedRevisionId) {
            auto guard = Guard(Lock_);
            if (PreparingFileSnapshot_) {
                for (const auto& [_, status] : currentTargetWorkerStatuses) {
                    if (status->PreparingFileSnapshot &&
                        status->PreparingFileSnapshot->SnapshotId == PreparingFileSnapshot_->Id &&
                        status->PreparingFileSnapshot->State == EFileSnapshotState::Validated)
                    {
                        ActiveFileSnapshot_ = PreparingFileSnapshot_;
                        PreparingFileSnapshot_.Reset();
                        ActiveFileSnapshotPublishedAt_ = TInstant::Now();
                        PersistFileSnapshotState();
                        break;
                    }
                }
            }
        }

        UpdateRolloutStatus(authoritativeWorkerStatuses, publishedRevisionId);

        auto guard = Guard(Lock_);
        for (auto it = FileSnapshotStateGauges_.begin(); it != FileSnapshotStateGauges_.end();) {
            if (!fileSnapshotStateCounts.contains(it->first)) {
                it->second.Update(0);
                auto toErase = it++;
                FileSnapshotStateGauges_.erase(toErase);
            } else {
                ++it;
            }
        }
        for (const auto& [key, count] : fileSnapshotStateCounts) {
            auto [it, inserted] = FileSnapshotStateGauges_.emplace(key, TGauge{});
            if (inserted) {
                it->second = Context_->Profiler
                    .WithTag("file_snapshot_id", ToString(key.first.Underlying()))
                    .WithTag("state", FormatEnum(key.second))
                    .Gauge("/file_snapshot_instance_count");
            }
            it->second.Update(count);
        }
        for (auto it = FileSourceRevisionStateGauges_.begin(); it != FileSourceRevisionStateGauges_.end();) {
            if (!fileSourceRevisionStateCounts.contains(it->first)) {
                it->second.Update(0);
                auto toErase = it++;
                FileSourceRevisionStateGauges_.erase(toErase);
            } else {
                ++it;
            }
        }
        for (const auto& [key, count] : fileSourceRevisionStateCounts) {
            auto [it, inserted] = FileSourceRevisionStateGauges_.emplace(key, TGauge{});
            if (inserted) {
                const auto& [fileSourceId, revisionId, state] = key;
                it->second = Context_->Profiler
                    .WithTag("file_source_id", fileSourceId.Underlying())
                    .WithTag("revision_id", revisionId.Underlying())
                    .WithTag("state", FormatEnum(state))
                    .Gauge("/file_source_revision_instance_count");
            }
            it->second.Update(count);
        }
        for (auto it = LiveAccessorCountGauges_.begin(); it != LiveAccessorCountGauges_.end();) {
            if (!liveAccessorCounts.contains(it->first)) {
                it->second.Update(0);
                auto toErase = it++;
                LiveAccessorCountGauges_.erase(toErase);
            } else {
                ++it;
            }
        }
        for (const auto& [snapshotId, count] : liveAccessorCounts) {
            auto [it, inserted] = LiveAccessorCountGauges_.emplace(snapshotId, TGauge{});
            if (inserted) {
                it->second = Context_->Profiler
                    .WithTag("file_snapshot_id", ToString(snapshotId.Underlying()))
                    .Gauge("/file_snapshot_live_accessor_count");
            }
            it->second.Update(count);
        }
        FileSnapshotStateCounts_ = std::move(fileSnapshotStateCounts);
        FileSourceRevisionStateCounts_ = std::move(fileSourceRevisionStateCounts);
        LiveAccessorCounts_ = std::move(liveAccessorCounts);
        UnknownFileSnapshotCount_ = unknownFileSnapshotCount;
        UnknownFileSnapshotCountGauge_.Update(unknownFileSnapshotCount);
    }

    IMapNodePtr GetView() const
    {
        if (Sources_.empty()) {
            return nullptr;
        }

        auto view = New<TFileSourceDiscoveryView>();
        {
            auto guard = Guard(Lock_);
            view->SourceRevisions = PublishedRevisions_;
            view->ActiveFileSnapshotId = ActiveFileSnapshot_
                ? std::optional(ActiveFileSnapshot_->Id)
                : std::nullopt;
            view->PreparingFileSnapshotId = PreparingFileSnapshot_
                ? std::optional(PreparingFileSnapshot_->Id)
                : std::nullopt;
            view->KnownFileSnapshotCount = std::ssize(KnownFileSnapshots_);
            view->UnknownFileSnapshotCount = UnknownFileSnapshotCount_;
            view->ActiveFileSnapshotPublishedAt = ActiveFileSnapshotPublishedAt_;
            view->FileSnapshotRolloutAge = FileSnapshotRolloutAge_;
            view->RolloutInstanceCount = RolloutInstanceCount_;
            view->RolloutConvergedInstanceCount = RolloutConvergedInstanceCount_;
            view->RolloutLaggingInstanceCount = RolloutLaggingInstanceCount_;
            view->RolloutUninitializedInstanceCount = RolloutUninitializedInstanceCount_;
            view->RolloutBlockingAccessorCount = RolloutBlockingAccessorCount_;
            view->RolloutProgressStateCounts = RolloutProgressStateCounts_;
            view->RolloutErrors = RolloutErrors_;
            for (const auto& [key, count] : FileSnapshotStateCounts_) {
                view->FileSnapshotStateCounts[Format("%v/%v", key.first, FormatEnum(key.second))] = count;
            }
            for (const auto& [key, count] : FileSourceRevisionStateCounts_) {
                const auto& [fileSourceId, revisionId, state] = key;
                view->FileSourceRevisionStateCounts[Format("%v/%v/%v", fileSourceId, revisionId, FormatEnum(state))] = count;
            }
            view->LiveAccessorCounts = LiveAccessorCounts_;
        }
        return ConvertToNode(view)->AsMap();
    }

private:
    struct TResourceInstanceIdentity
    {
        TIncarnationId WorkerIncarnationId;
        TResourceInstanceId ResourceInstanceId;
        ui64 Generation;
    };

    struct TSourceEntry
    {
        TFileSourceSpecPtr Spec;
        TDynamicFileSourceSpecPtr DynamicSpec;
        IFileSourcePtr Source;
        IStatusErrorStatePtr DiscoveryError;
        TPeriodicExecutorPtr DiscoveryExecutor;
        ui64 Generation = 0;
    };

    void UpdateRolloutStatus(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        std::optional<i64> publishedRevisionId)
    {
        TFileSnapshotPtr activeFileSnapshot;
        std::optional<TInstant> publishedAt;
        TDuration warningPeriod;
        {
            auto guard = Guard(Lock_);
            activeFileSnapshot = ActiveFileSnapshot_;
            publishedAt = ActiveFileSnapshotPublishedAt_;
            warningPeriod = FileSnapshotRolloutWarningPeriod_;
        }

        i64 convergedCount = 0;
        i64 laggingCount = 0;
        i64 uninitializedCount = 0;
        i64 blockingAccessorCount = 0;
        THashMap<std::string, i64> progressStateCounts;
        THashMap<std::string, TError> rolloutErrors;
        for (const auto& [workerAddress, status] : workerStatuses) {
            const bool hasCurrentTarget = publishedRevisionId &&
                status->TargetRevisionId == publishedRevisionId;
            if (activeFileSnapshot &&
                hasCurrentTarget &&
                status->ActiveFileSnapshotId == activeFileSnapshot->Id)
            {
                ++convergedCount;
                continue;
            }

            if (status->ActiveFileSnapshotId) {
                ++laggingCount;
            } else {
                ++uninitializedCount;
            }

            if (!hasCurrentTarget) {
                ++progressStateCounts["target_revision_pending"];
            } else if (status->PreparingFileSnapshot) {
                auto progressState = status->PreparingFileSnapshot->PreparationStage
                    ? FormatEnum(*status->PreparingFileSnapshot->PreparationStage)
                    : FormatEnum(status->PreparingFileSnapshot->State);
                ++progressStateCounts[progressState];
                if (!status->PreparingFileSnapshot->Error.IsOK()) {
                    rolloutErrors[workerAddress] = status->PreparingFileSnapshot->Error;
                }
                if (activeFileSnapshot &&
                    status->PreparingFileSnapshot->SnapshotId == activeFileSnapshot->Id &&
                    status->PreparingFileSnapshot->State == EFileSnapshotState::Draining)
                {
                    for (const auto& [snapshotId, count] : status->LiveAccessorCounts) {
                        if (snapshotId != activeFileSnapshot->Id && count > 0) {
                            blockingAccessorCount += count;
                        }
                    }
                }
            } else {
                ++progressStateCounts["idle"];
            }
        }

        auto now = TInstant::Now();
        std::optional<TDuration> rolloutAge;
        if (publishedAt) {
            rolloutAge = now >= *publishedAt
                ? now - *publishedAt
                : TDuration::Zero();
        }
        const auto instanceCount = static_cast<i64>(workerStatuses.size());
        const bool shouldWarn = activeFileSnapshot &&
            publishedRevisionId &&
            rolloutAge &&
            instanceCount > 0 &&
            convergedCount != instanceCount &&
            *rolloutAge >= warningPeriod;

        {
            auto guard = Guard(Lock_);
            FileSnapshotRolloutAge_ = rolloutAge;
            RolloutInstanceCount_ = instanceCount;
            RolloutConvergedInstanceCount_ = convergedCount;
            RolloutLaggingInstanceCount_ = laggingCount;
            RolloutUninitializedInstanceCount_ = uninitializedCount;
            RolloutBlockingAccessorCount_ = blockingAccessorCount;
            RolloutProgressStateCounts_ = progressStateCounts;
            RolloutErrors_ = rolloutErrors;
        }

        if (shouldWarn) {
            RolloutWarningErrorState_->SetError(
                TError("File snapshot rollout has not converged")
                    .With("active_file_snapshot_id", activeFileSnapshot->Id)
                    .With("rollout_age", *rolloutAge)
                    .With("instance_count", instanceCount)
                    .With("converged_instance_count", convergedCount)
                    .With("lagging_instance_count", laggingCount)
                    .With("uninitialized_instance_count", uninitializedCount)
                    .With("blocking_accessor_count", blockingAccessorCount)
                    .With("progress_state_counts", progressStateCounts));
        } else {
            RolloutWarningErrorState_->ClearError();
        }
    }

    THashMap<std::string, TWorkerResourceStatusPtr> FilterAuthoritativeStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses)
    {
        THashMap<std::string, TWorkerResourceStatusPtr> result;
        auto guard = Guard(Lock_);
        for (auto it = ResourceInstances_.begin(); it != ResourceInstances_.end();) {
            if (!workerStatuses.contains(it->first)) {
                ResourceInstances_.erase(it++);
            } else {
                ++it;
            }
        }
        for (const auto& [workerAddress, workerStatus] : workerStatuses) {
            if (!workerStatus || !workerStatus->WorkerIncarnationId) {
                continue;
            }
            auto statusIt = workerStatus->ResourceStatuses.find(Context_->ResourceId);
            if (statusIt == workerStatus->ResourceStatuses.end() || !statusIt->second) {
                continue;
            }
            const auto& status = statusIt->second;
            if (!status->ResourceInstanceId || !status->ResourceIncarnationGeneration)
            {
                continue;
            }

            TResourceInstanceIdentity observed{
                .WorkerIncarnationId = *workerStatus->WorkerIncarnationId,
                .ResourceInstanceId = *status->ResourceInstanceId,
                .Generation = *status->ResourceIncarnationGeneration,
            };
            auto [it, inserted] = ResourceInstances_.emplace(workerAddress, observed);
            bool isAuthoritative = inserted;
            if (!inserted) {
                if (it->second.WorkerIncarnationId != observed.WorkerIncarnationId ||
                    observed.Generation > it->second.Generation)
                {
                    it->second = observed;
                    isAuthoritative = true;
                } else if (observed.Generation == it->second.Generation &&
                    observed.ResourceInstanceId == it->second.ResourceInstanceId)
                {
                    isAuthoritative = true;
                }
            }

            if (isAuthoritative) {
                result.emplace(workerAddress, status);
            }
        }
        return result;
    }

    static bool AreFileSourceRevisionsEqual(
        const THashMap<TFileSourceId, TFileSourceRevisionPtr>& lhs,
        const THashMap<TFileSourceId, TFileSourceRevisionPtr>& rhs)
    {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (const auto& [id, revision] : lhs) {
            auto it = rhs.find(id);
            if (it == rhs.end() ||
                !AreNodesEqual(ConvertToNode(revision), ConvertToNode(it->second)))
            {
                return false;
            }
        }
        return true;
    }

    static bool MatchesFileSnapshot(
        const TFileSnapshotPtr& snapshot,
        const THashMap<TFileSourceId, TFileSourceRevisionPtr>& revisions)
    {
        return snapshot && AreFileSourceRevisionsEqual(snapshot->FileSources, revisions);
    }

    bool IsFileSnapshotCompatible(const TFileSnapshotPtr& snapshot) const
    {
        if (!snapshot) {
            return true;
        }
        if (snapshot->FileSources.size() != Sources_.size()) {
            return false;
        }
        for (const auto& [id, entry] : Sources_) {
            auto it = snapshot->FileSources.find(id);
            if (it == snapshot->FileSources.end() ||
                !it->second ||
                it->second->FileSourceClassName != entry.Spec->FileSourceClassName)
            {
                return false;
            }
        }
        return true;
    }

    TFileSnapshotPtr FindKnownFileSnapshot(std::optional<TFileSnapshotId> id) const
    {
        if (!id) {
            return nullptr;
        }
        auto it = KnownFileSnapshots_.find(*id);
        return it == KnownFileSnapshots_.end() ? nullptr : it->second;
    }

    bool ArePersistedFileSnapshotsCompatible(
        const TFileSnapshotPtr& activeFileSnapshot,
        const TFileSnapshotPtr& preparingFileSnapshot) const
    {
        return (!State_->ActiveFileSnapshotId || activeFileSnapshot) &&
            (!State_->PreparingFileSnapshotId || preparingFileSnapshot) &&
            IsFileSnapshotCompatible(activeFileSnapshot) &&
            IsFileSnapshotCompatible(preparingFileSnapshot) &&
            (!activeFileSnapshot ||
            !preparingFileSnapshot ||
            activeFileSnapshot->Id != preparingFileSnapshot->Id);
    }

    void RegisterKnownFileSnapshot(const TFileSnapshotPtr& snapshot)
    {
        if (snapshot) {
            KnownFileSnapshots_[snapshot->Id] = snapshot;
        }
        PruneKnownFileSnapshots();
    }

    void PruneKnownFileSnapshots()
    {
        while (static_cast<i64>(KnownFileSnapshots_.size()) > FileSnapshotCatalogMaxEntries_) {
            auto victim = KnownFileSnapshots_.end();
            for (auto it = KnownFileSnapshots_.begin(); it != KnownFileSnapshots_.end(); ++it) {
                if ((ActiveFileSnapshot_ && it->first == ActiveFileSnapshot_->Id) ||
                    (PreparingFileSnapshot_ && it->first == PreparingFileSnapshot_->Id))
                {
                    continue;
                }
                if (victim == KnownFileSnapshots_.end() ||
                    it->first.Underlying() < victim->first.Underlying())
                {
                    victim = it;
                }
            }
            YT_VERIFY(victim != KnownFileSnapshots_.end());
            KnownFileSnapshots_.erase(victim);
        }
    }

    void PersistFileSnapshotState()
    {
        if (!State_.IsInitialized()) {
            return;
        }
        RegisterKnownFileSnapshot(ActiveFileSnapshot_);
        RegisterKnownFileSnapshot(PreparingFileSnapshot_);
        State_->ActiveFileSnapshotId = ActiveFileSnapshot_
            ? std::optional(ActiveFileSnapshot_->Id)
            : std::nullopt;
        State_->PreparingFileSnapshotId = PreparingFileSnapshot_
            ? std::optional(PreparingFileSnapshot_->Id)
            : std::nullopt;
        State_->KnownFileSnapshots = KnownFileSnapshots_;
        State_->LastFileSnapshotCreationTime = LastFileSnapshotCreationTime_;
        State_->ActiveFileSnapshotPublishedAt = ActiveFileSnapshotPublishedAt_;
    }

    void Discover(const TFileSourceId& id)
    {
        const auto& Logger = Context_->Logger;
        const auto& entry = GetOrCrash(Sources_, id);
        ui64 generation;
        {
            auto guard = Guard(Lock_);
            generation = entry.Generation;
        }
        try {
            auto revision = WaitFor(entry.Source->Discover()).ValueOrThrow();
            if (revision) {
                THROW_ERROR_EXCEPTION_UNLESS(
                    revision->FileSourceClassName == entry.Spec->FileSourceClassName,
                    "Discovered file source %Qv class %Qv differs from configured class %Qv",
                    id,
                    revision->FileSourceClassName,
                    entry.Spec->FileSourceClassName);
                {
                    auto guard = Guard(Lock_);
                    if (generation != entry.Generation) {
                        return;
                    }
                    PendingRevisions_[id] = revision;
                    if (PendingRevisions_.size() == Sources_.size()) {
                        PublishedRevisions_ = PendingRevisions_;
                    }
                    if (State_.IsInitialized()) {
                        State_->Revisions = PendingRevisions_;
                    }
                }
                entry.DiscoveryError->ClearError();
                return;
            }

            bool hasRevision;
            {
                auto guard = Guard(Lock_);
                if (generation != entry.Generation) {
                    return;
                }
                hasRevision = PendingRevisions_.contains(id);
            }
            if (hasRevision) {
                entry.DiscoveryError->ClearError();
            } else {
                entry.DiscoveryError->SetError(
                    TError("File source discovery returned no revision")
                        .With("file_source", id));
            }
        } catch (const std::exception& ex) {
            auto error = TError("File source discovery failed")
                .With("file_source", id)
                .With(TError(ex));
            {
                auto guard = Guard(Lock_);
                if (generation != entry.Generation) {
                    return;
                }
            }
            entry.DiscoveryError->SetError(error);
            YT_TLOG_WARNING("File source discovery failed")
                .With("FileSource", id)
                .With(error);
        }
    }

    const TResourceControllerContextPtr Context_;
    THashMap<TFileSourceId, TSourceEntry> Sources_;

    mutable NThreading::TSpinLock Lock_;
    TMutableStateClient<TFileSourceDiscoveryState> State_;
    IMapNodePtr DynamicFileSources_;
    THashMap<TFileSourceId, TFileSourceRevisionPtr> PendingRevisions_;
    THashMap<TFileSourceId, TFileSourceRevisionPtr> PublishedRevisions_;
    TFileSnapshotPtr ActiveFileSnapshot_;
    TFileSnapshotPtr PreparingFileSnapshot_;
    THashMap<TFileSnapshotId, TFileSnapshotPtr> KnownFileSnapshots_;
    std::optional<TInstant> LastFileSnapshotCreationTime_;
    std::optional<TInstant> ActiveFileSnapshotPublishedAt_;
    TDuration FileSnapshotMinCreationPeriod_;
    i64 FileSnapshotCatalogMaxEntries_;
    TDuration FileSnapshotRolloutWarningPeriod_;
    IStatusErrorStatePtr RolloutWarningErrorState_;
    THashMap<std::string, TResourceInstanceIdentity> ResourceInstances_;
    THashMap<std::pair<TFileSnapshotId, EFileSnapshotState>, i64> FileSnapshotStateCounts_;
    THashMap<std::tuple<TFileSourceId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, i64> FileSourceRevisionStateCounts_;
    THashMap<std::pair<TFileSnapshotId, EFileSnapshotState>, TGauge> FileSnapshotStateGauges_;
    THashMap<std::tuple<TFileSourceId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, TGauge> FileSourceRevisionStateGauges_;
    THashMap<TFileSnapshotId, i64> LiveAccessorCounts_;
    THashMap<TFileSnapshotId, TGauge> LiveAccessorCountGauges_;
    std::optional<TDuration> FileSnapshotRolloutAge_;
    i64 RolloutInstanceCount_ = 0;
    i64 RolloutConvergedInstanceCount_ = 0;
    i64 RolloutLaggingInstanceCount_ = 0;
    i64 RolloutUninitializedInstanceCount_ = 0;
    i64 RolloutBlockingAccessorCount_ = 0;
    THashMap<std::string, i64> RolloutProgressStateCounts_;
    THashMap<std::string, TError> RolloutErrors_;
    i64 UnknownFileSnapshotCount_ = 0;
    TGauge UnknownFileSnapshotCountGauge_;
};

////////////////////////////////////////////////////////////////////////////////

TResourceControllerBase::~TResourceControllerBase() = default;

TResourceControllerBase::TResourceControllerBase(
    TResourceControllerContextPtr context,
    TDynamicResourceControllerContextPtr dynamicContext)
    : Context_(std::move(context))
    , DynamicContext_(dynamicContext)
    , Parameters_(TRegistry::Get()->ParseResourceParameters(Context_->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(
        Context_->ResourceSpec,
        dynamicContext->DynamicResourceSpec))
    , FileSourceDiscovery_(New<TFileSourceDiscovery>(Context_, dynamicContext))
    , Logger(Context_->Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicResourceControllerContextPtr& newDynamicContext) {
        DynamicContext_ = newDynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(
            Context_->ResourceSpec,
            newDynamicContext->DynamicResourceSpec);
        FileSourceDiscovery_->Reconfigure(newDynamicContext);
    }));
}

void TResourceControllerBase::Init(IInitContextPtr initContext)
{
    DoInit(initContext ? initContext->WithPrefix("controller") : nullptr);
    FileSourceDiscovery_->Init(initContext ? initContext->WithPrefix("file_sources") : nullptr);
}

TResourceRevisionPtr TResourceControllerBase::BuildTargetRevision()
{
    auto spec = DoBuildTargetRevisionSpec();
    auto fileSnapshots = FileSourceDiscovery_->BuildTargetFileSnapshots();
    if (!spec &&
        (!fileSnapshots || (!fileSnapshots->first && !fileSnapshots->second)))
    {
        return nullptr;
    }

    auto revision = New<TResourceRevision>();
    revision->Spec = std::move(spec);
    if (fileSnapshots) {
        revision->ActiveFileSnapshot = fileSnapshots->first;
        revision->PreparingFileSnapshot = fileSnapshots->second;
    }
    return revision;
}

void TResourceControllerBase::CollectStatuses(
    const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
    const TWorkerResourceStatusPtr& controllerStatus,
    std::optional<i64> publishedRevisionId)
{
    THashMap<std::string, TWorkerResourceStatusPtr> resourceStatuses;
    for (const auto& [workerAddress, workerStatus] : workerStatuses) {
        if (!workerStatus) {
            continue;
        }
        auto it = workerStatus->ResourceStatuses.find(Context_->ResourceId);
        if (it != workerStatus->ResourceStatuses.end() && it->second) {
            resourceStatuses.emplace(workerAddress, it->second);
        }
    }
    DoCollectStatuses(resourceStatuses, controllerStatus);
    FileSourceDiscovery_->CollectStatuses(workerStatuses, publishedRevisionId);
}

IMapNodePtr TResourceControllerBase::GetView()
{
    auto view = DoGetView();
    if (auto fileSourcesView = FileSourceDiscovery_->GetView()) {
        if (!view) {
            view = GetEphemeralNodeFactory()->CreateMap();
        }
        THROW_ERROR_EXCEPTION_IF(
            view->FindChild("file_sources"),
            "Resource controller view uses reserved child \"file_sources\"");
        YT_VERIFY(view->AddChild("file_sources", std::move(fileSourcesView)));
    }
    return view;
}

void TResourceControllerBase::DoInit(IInitContextPtr /*initContext*/)
{ }

INodePtr TResourceControllerBase::DoBuildTargetRevisionSpec()
{
    return nullptr;
}

void TResourceControllerBase::DoCollectStatuses(
    const THashMap<std::string, TWorkerResourceStatusPtr>& /*workerStatuses*/,
    const TWorkerResourceStatusPtr& /*controllerStatus*/)
{ }

IMapNodePtr TResourceControllerBase::DoGetView()
{
    return nullptr;
}

TResourceControllerContextPtr TResourceControllerBase::GetContext() const
{
    return Context_;
}

TDynamicResourceControllerContextPtr TResourceControllerBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TResourceSpecPtr TResourceControllerBase::GetSpec() const
{
    return Context_->ResourceSpec;
}

TDynamicResourceSpecPtr TResourceControllerBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicResourceSpec;
}

NYTree::TYsonStructPtr TResourceControllerBase::GetParametersBase() const
{
    return Parameters_;
}

NYTree::TYsonStructPtr TResourceControllerBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
