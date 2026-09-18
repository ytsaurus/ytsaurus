#include "resource_controller_base.h"

#include <yt/yt/flow/library/cpp/common/file_provider.h>
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

struct TFileProviderDiscoveryState
    : public TYsonStruct
{
    IMapNodePtr FileProviders;
    IMapNodePtr DynamicFileProviders;
    THashMap<TFileProviderId, TFileProviderRevisionPtr> Revisions;
    std::optional<TFileSnapshotId> ActiveFileSnapshotId;
    std::optional<TFileSnapshotId> PreparingFileSnapshotId;
    THashMap<TFileSnapshotId, TFileSnapshotPtr> KnownFileSnapshots;
    std::optional<TInstant> LastFileSnapshotCreationTime;
    std::optional<TInstant> ActiveFileSnapshotPublishedAt;

    REGISTER_YSON_STRUCT(TFileProviderDiscoveryState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("file_providers", &TThis::FileProviders)
            .Default();
        registrar.Parameter("dynamic_file_providers", &TThis::DynamicFileProviders)
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

struct TFileProviderDiscoveryView
    : public TYsonStruct
{
    THashMap<TFileProviderId, TFileProviderRevisionPtr> ProviderRevisions;
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
    THashMap<std::string, i64> FileProviderRevisionStateCounts;
    THashMap<TFileSnapshotId, i64> LiveAccessorCounts;

    REGISTER_YSON_STRUCT(TFileProviderDiscoveryView);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("provider_revisions", &TThis::ProviderRevisions)
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
        registrar.Parameter("file_provider_revision_state_counts", &TThis::FileProviderRevisionStateCounts)
            .Default();
        registrar.Parameter("live_accessor_counts", &TThis::LiveAccessorCounts)
            .Default();
    }
};

TDynamicFileProviderSpecPtr GetDynamicFileProviderSpec(
    const TDynamicResourceSpecPtr& dynamicResourceSpec,
    const TFileProviderId& id)
{
    if (auto it = dynamicResourceSpec->FileProviders.find(id);
        it != dynamicResourceSpec->FileProviders.end())
    {
        return it->second;
    }

    auto result = New<TDynamicFileProviderSpec>();
    result->Parameters = GetEphemeralNodeFactory()->CreateMap();
    return result;
}

THashMap<TFileProviderId, TDynamicFileProviderSpecPtr> BuildDynamicFileProviderSpecs(
    const TResourceSpecPtr& resourceSpec,
    const TDynamicResourceSpecPtr& dynamicResourceSpec)
{
    THashMap<TFileProviderId, TDynamicFileProviderSpecPtr> result;
    for (const auto& [id, _] : resourceSpec->FileProviders) {
        EmplaceOrCrash(result, id, GetDynamicFileProviderSpec(dynamicResourceSpec, id));
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TResourceControllerBase::TFileProviderDiscovery
    : public TRefCounted
{
public:
    TFileProviderDiscovery(
        TResourceControllerContextPtr context,
        const TDynamicResourceControllerContextPtr& dynamicContext)
        : Context_(std::move(context))
        , FileSnapshotMinCreationPeriod_(dynamicContext->DynamicResourceSpec->FileSnapshotMinCreationPeriod)
        , FileSnapshotCatalogMaxEntries_(dynamicContext->DynamicResourceSpec->FileSnapshotCatalogMaxEntries)
        , FileSnapshotRolloutWarningPeriod_(dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod)
    {
        if (!Context_->ResourceSpec->FileProviders.empty()) {
            UnknownFileSnapshotCountGauge_ = Context_->Profiler.Gauge("/unknown_file_snapshot_count");
            RolloutWarningErrorState_ = Context_->StatusProfiler->ErrorState("/file_snapshot_rollout");
        }

        auto dynamicFileProviders = BuildDynamicFileProviderSpecs(
            Context_->ResourceSpec,
            dynamicContext->DynamicResourceSpec);
        DynamicFileProviders_ = ConvertToNode(dynamicFileProviders)->AsMap();

        for (const auto& [id, spec] : Context_->ResourceSpec->FileProviders) {
            auto providerContext = New<TFileProviderContext>();
            providerContext->ProviderSpec = spec;
            providerContext->PipelineAuthenticator = Context_->PipelineAuthenticator;
            providerContext->ClientsCache = Context_->ClientsCache;
            providerContext->HttpClient = Context_->HttpClient;
            providerContext->PipelinePath = Context_->PipelinePath;
            providerContext->Invoker = Context_->Invoker;
            providerContext->Logger = Context_->Logger
                .WithTag("Component", "FileProvider")
                .WithTag("FileProvider", id);

            auto errorState = Context_->StatusProfiler->ErrorState(
                Format("/file_providers/%v/discovery", id));
            auto dynamicProviderContext = New<TDynamicFileProviderContext>();
            dynamicProviderContext->DynamicFileProviderSpec = GetOrCrash(dynamicFileProviders, id);
            auto executor = New<TPeriodicExecutor>(
                Context_->Invoker,
                BIND(&TFileProviderDiscovery::Discover, MakeWeak(this), id),
                dynamicContext->DynamicResourceSpec->FileProviderDiscoverPeriod);
            TProviderEntry providerEntry{
                .Spec = spec,
                .DynamicSpec = dynamicProviderContext->DynamicFileProviderSpec,
                .Provider = TRegistry::Get()->CreateFileProvider(providerContext, dynamicProviderContext),
                .DiscoveryError = std::move(errorState),
                .DiscoveryExecutor = std::move(executor),
            };
            EmplaceOrCrash(Providers_, id, std::move(providerEntry));
        }
    }

    void Init(const IInitContextPtr& initContext)
    {
        if (initContext) {
            initContext->InitClient<TFileProviderDiscoveryState>(State_, "v0");

            auto fileProviders = ConvertToNode(Context_->ResourceSpec->FileProviders)->AsMap();
            if (State_->FileProviders &&
                State_->DynamicFileProviders &&
                AreNodesEqual(State_->FileProviders, fileProviders) &&
                AreNodesEqual(State_->DynamicFileProviders, DynamicFileProviders_))
            {
                auto guard = Guard(Lock_);
                for (const auto& [id, revision] : State_->Revisions) {
                    auto providerIt = Providers_.find(id);
                    if (providerIt != Providers_.end() &&
                        revision &&
                        revision->FileProviderClassName == providerIt->second.Spec->FileProviderClassName)
                    {
                        PendingRevisions_[id] = revision;
                    }
                }
                if (PendingRevisions_.size() == Providers_.size()) {
                    PublishedRevisions_ = PendingRevisions_;
                }
            } else {
                State_->FileProviders = std::move(fileProviders);
                State_->DynamicFileProviders = DynamicFileProviders_;
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

        for (const auto& [_, entry] : Providers_) {
            entry.DiscoveryExecutor->Start();
        }
    }

    void Reconfigure(const TDynamicResourceControllerContextPtr& dynamicContext)
    {
        auto dynamicFileProviders = BuildDynamicFileProviderSpecs(
            Context_->ResourceSpec,
            dynamicContext->DynamicResourceSpec);
        auto dynamicFileProvidersNode = ConvertToNode(dynamicFileProviders)->AsMap();
        std::vector<TPeriodicExecutorPtr> changedExecutors;

        for (auto& [id, entry] : Providers_) {
            entry.DiscoveryExecutor->SetPeriod(
                dynamicContext->DynamicResourceSpec->FileProviderDiscoverPeriod);

            const auto& dynamicSpec = GetOrCrash(dynamicFileProviders, id);
            if (AreNodesEqual(ConvertToNode(entry.DynamicSpec), ConvertToNode(dynamicSpec))) {
                continue;
            }

            auto dynamicProviderContext = New<TDynamicFileProviderContext>();
            dynamicProviderContext->DynamicFileProviderSpec = dynamicSpec;
            entry.Provider->Reconfigure(dynamicProviderContext);
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
            if (!AreNodesEqual(DynamicFileProviders_, dynamicFileProvidersNode)) {
                DynamicFileProviders_ = std::move(dynamicFileProvidersNode);
                if (State_.IsInitialized()) {
                    State_->DynamicFileProviders = DynamicFileProviders_;
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
        std::optional<THashMap<TFileProviderId, TFileProviderRevisionPtr>> revisionsToSnapshot;
        {
            auto guard = Guard(Lock_);
            auto now = TInstant::Now();
            if (LastFileSnapshotCreationTime_ && *LastFileSnapshotCreationTime_ > now) {
                LastFileSnapshotCreationTime_ = now;
                PersistFileSnapshotState();
            }
            if (PublishedRevisions_.size() == Providers_.size() &&
                MatchesFileSnapshot(ActiveFileSnapshot_, PublishedRevisions_) &&
                PreparingFileSnapshot_)
            {
                PreparingFileSnapshot_.Reset();
                PersistFileSnapshotState();
            }
            if (!Providers_.empty() &&
                PublishedRevisions_.size() == Providers_.size() &&
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
                "File provider controller requires a time provider");
            auto snapshot = New<TFileSnapshot>();
            snapshot->Id = TFileSnapshotId(Context_->TimeProvider->GenerateSeqNo());
            snapshot->FileProviders = *revisionsToSnapshot;

            auto guard = Guard(Lock_);
            auto now = TInstant::Now();
            if (AreFileProviderRevisionsEqual(PublishedRevisions_, *revisionsToSnapshot) &&
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
        if (!Providers_.empty() && !ActiveFileSnapshot_ && !PreparingFileSnapshot_) {
            return std::nullopt;
        }
        return std::pair(ActiveFileSnapshot_, PreparingFileSnapshot_);
    }

    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        std::optional<i64> publishedRevisionId)
    {
        if (Providers_.empty()) {
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
        THashMap<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, i64> fileProviderRevisionStateCounts;
        THashMap<TFileSnapshotId, i64> liveAccessorCounts;
        i64 unknownFileSnapshotCount = 0;
        for (const auto& [_, status] : authoritativeWorkerStatuses) {
            THashMap<TFileSnapshotId, EFileSnapshotState> workerFileSnapshotStates;
            THashMap<std::pair<TFileProviderId, NFileStorage::TFileStorageObjectId>, EFileSnapshotState> workerFileProviderRevisionStates;
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
                for (const auto& [fileProviderId, revision] : snapshotIt->second->FileProviders) {
                    if (!revision) {
                        continue;
                    }
                    auto key = std::pair(fileProviderId, revision->ObjectId);
                    auto [revisionIt, revisionInserted] = workerFileProviderRevisionStates.emplace(key, state);
                    if (!revisionInserted && state > revisionIt->second) {
                        revisionIt->second = state;
                    }
                }
            }
            for (const auto& [key, state] : workerFileProviderRevisionStates) {
                ++fileProviderRevisionStateCounts[std::tuple(key.first, key.second, state)];
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
        for (auto it = FileProviderRevisionStateGauges_.begin(); it != FileProviderRevisionStateGauges_.end();) {
            if (!fileProviderRevisionStateCounts.contains(it->first)) {
                it->second.Update(0);
                auto toErase = it++;
                FileProviderRevisionStateGauges_.erase(toErase);
            } else {
                ++it;
            }
        }
        for (const auto& [key, count] : fileProviderRevisionStateCounts) {
            auto [it, inserted] = FileProviderRevisionStateGauges_.emplace(key, TGauge{});
            if (inserted) {
                const auto& [fileProviderId, revisionId, state] = key;
                it->second = Context_->Profiler
                    .WithTag("file_provider_id", fileProviderId.Underlying())
                    .WithTag("revision_id", revisionId.Underlying())
                    .WithTag("state", FormatEnum(state))
                    .Gauge("/file_provider_revision_instance_count");
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
        FileProviderRevisionStateCounts_ = std::move(fileProviderRevisionStateCounts);
        LiveAccessorCounts_ = std::move(liveAccessorCounts);
        UnknownFileSnapshotCount_ = unknownFileSnapshotCount;
        UnknownFileSnapshotCountGauge_.Update(unknownFileSnapshotCount);
    }

    IMapNodePtr GetView() const
    {
        if (Providers_.empty()) {
            return nullptr;
        }

        auto view = New<TFileProviderDiscoveryView>();
        {
            auto guard = Guard(Lock_);
            view->ProviderRevisions = PublishedRevisions_;
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
            for (const auto& [key, count] : FileProviderRevisionStateCounts_) {
                const auto& [fileProviderId, revisionId, state] = key;
                view->FileProviderRevisionStateCounts[Format("%v/%v/%v", fileProviderId, revisionId, FormatEnum(state))] = count;
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

    struct TProviderEntry
    {
        TFileProviderSpecPtr Spec;
        TDynamicFileProviderSpecPtr DynamicSpec;
        IFileProviderPtr Provider;
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

    static bool AreFileProviderRevisionsEqual(
        const THashMap<TFileProviderId, TFileProviderRevisionPtr>& lhs,
        const THashMap<TFileProviderId, TFileProviderRevisionPtr>& rhs)
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
        const THashMap<TFileProviderId, TFileProviderRevisionPtr>& revisions)
    {
        return snapshot && AreFileProviderRevisionsEqual(snapshot->FileProviders, revisions);
    }

    bool IsFileSnapshotCompatible(const TFileSnapshotPtr& snapshot) const
    {
        if (!snapshot) {
            return true;
        }
        if (snapshot->FileProviders.size() != Providers_.size()) {
            return false;
        }
        for (const auto& [id, entry] : Providers_) {
            auto it = snapshot->FileProviders.find(id);
            if (it == snapshot->FileProviders.end() ||
                !it->second ||
                it->second->FileProviderClassName != entry.Spec->FileProviderClassName)
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

    void Discover(const TFileProviderId& id)
    {
        const auto& Logger = Context_->Logger;
        const auto& entry = GetOrCrash(Providers_, id);
        ui64 generation;
        {
            auto guard = Guard(Lock_);
            generation = entry.Generation;
        }
        try {
            auto revision = WaitFor(entry.Provider->Discover()).ValueOrThrow();
            if (revision) {
                THROW_ERROR_EXCEPTION_UNLESS(
                    revision->FileProviderClassName == entry.Spec->FileProviderClassName,
                    "Discovered file provider %Qv class %Qv differs from configured class %Qv",
                    id,
                    revision->FileProviderClassName,
                    entry.Spec->FileProviderClassName);
                {
                    auto guard = Guard(Lock_);
                    if (generation != entry.Generation) {
                        return;
                    }
                    PendingRevisions_[id] = revision;
                    if (PendingRevisions_.size() == Providers_.size()) {
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
                    TError("File provider discovery returned no revision")
                        .With("file_provider", id));
            }
        } catch (const std::exception& ex) {
            auto error = TError("File provider discovery failed")
                .With("file_provider", id)
                .With(TError(ex));
            {
                auto guard = Guard(Lock_);
                if (generation != entry.Generation) {
                    return;
                }
            }
            entry.DiscoveryError->SetError(error);
            YT_TLOG_WARNING("File provider discovery failed")
                .With("FileProvider", id)
                .With(error);
        }
    }

    const TResourceControllerContextPtr Context_;
    THashMap<TFileProviderId, TProviderEntry> Providers_;

    mutable NThreading::TSpinLock Lock_;
    TMutableStateClient<TFileProviderDiscoveryState> State_;
    IMapNodePtr DynamicFileProviders_;
    THashMap<TFileProviderId, TFileProviderRevisionPtr> PendingRevisions_;
    THashMap<TFileProviderId, TFileProviderRevisionPtr> PublishedRevisions_;
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
    THashMap<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, i64> FileProviderRevisionStateCounts_;
    THashMap<std::pair<TFileSnapshotId, EFileSnapshotState>, TGauge> FileSnapshotStateGauges_;
    THashMap<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, EFileSnapshotState>, TGauge> FileProviderRevisionStateGauges_;
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
    , FileProviderDiscovery_(New<TFileProviderDiscovery>(Context_, dynamicContext))
    , Logger(Context_->Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicResourceControllerContextPtr& newDynamicContext) {
        DynamicContext_ = newDynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(
            Context_->ResourceSpec,
            newDynamicContext->DynamicResourceSpec);
        FileProviderDiscovery_->Reconfigure(newDynamicContext);
    }));
}

void TResourceControllerBase::Init(IInitContextPtr initContext)
{
    DoInit(initContext ? initContext->WithPrefix("controller") : nullptr);
    FileProviderDiscovery_->Init(initContext ? initContext->WithPrefix("file_providers") : nullptr);
}

TResourceRevisionPtr TResourceControllerBase::BuildTargetRevision()
{
    auto spec = DoBuildTargetRevisionSpec();
    auto fileSnapshots = FileProviderDiscovery_->BuildTargetFileSnapshots();
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
    FileProviderDiscovery_->CollectStatuses(workerStatuses, publishedRevisionId);
}

IMapNodePtr TResourceControllerBase::GetView()
{
    auto view = DoGetView();
    if (auto fileProvidersView = FileProviderDiscovery_->GetView()) {
        if (!view) {
            view = GetEphemeralNodeFactory()->CreateMap();
        }
        THROW_ERROR_EXCEPTION_IF(
            view->FindChild("file_providers"),
            "Resource controller view uses reserved child \"file_providers\"");
        YT_VERIFY(view->AddChild("file_providers", std::move(fileProvidersView)));
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
