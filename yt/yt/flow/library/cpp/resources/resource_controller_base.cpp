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

#include <library/cpp/yt/system/spin_lock.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TFileProviderDiscoveryState
    : public TYsonStruct
{
    IMapNodePtr ResourceSpec;
    IMapNodePtr DynamicResourceSpec;
    std::optional<TFileSnapshotId> ActiveFileSnapshotId;
    std::optional<TFileSnapshotId> PreparingFileSnapshotId;
    THashMap<TFileSnapshotId, TFileSnapshotPtr> KnownFileSnapshots;
    std::optional<TInstant> LastFileSnapshotCreationTime;
    std::optional<TInstant> ActiveFileSnapshotPublishedAt;

    REGISTER_YSON_STRUCT(TFileProviderDiscoveryState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("resource_spec", &TThis::ResourceSpec)
            .Default();
        registrar.Parameter("dynamic_resource_spec", &TThis::DynamicResourceSpec)
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

        DynamicResourceSpec_ = ConvertToNode(dynamicContext->DynamicResourceSpec)->AsMap();

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

            auto dynamicProviderContext = New<TDynamicFileProviderContext>();
            dynamicProviderContext->DynamicFileProviderSpec = GetDynamicFileProviderSpec(
                dynamicContext->DynamicResourceSpec,
                id);
            EmplaceOrCrash(Providers_, id, TRegistry::Get()->CreateFileProvider(providerContext, dynamicProviderContext));
        }

        if (!Providers_.empty()) {
            DiscoveryError_ = Context_->StatusProfiler->ErrorState("/file_providers/discovery");
            DiscoveryExecutor_ = New<TPeriodicExecutor>(
                Context_->Invoker,
                BIND(&TFileProviderDiscovery::Discover, MakeWeak(this)),
                dynamicContext->DynamicResourceSpec->FileProviderDiscoverPeriod);
        }
    }

    void Init(const IInitContextPtr& initContext)
    {
        if (initContext) {
            initContext->InitClient<TFileProviderDiscoveryState>(State_, "v0");

            auto resourceSpec = ConvertToNode(Context_->ResourceSpec)->AsMap();
            const auto staticSpecMatches = State_->ResourceSpec &&
                AreNodesEqual(State_->ResourceSpec, resourceSpec);
            ForceActive_ = State_->ResourceSpec && !staticSpecMatches;
            const bool discoverySpecMatches = staticSpecMatches &&
                State_->DynamicResourceSpec &&
                AreNodesEqual(State_->DynamicResourceSpec, DynamicResourceSpec_);

            {
                auto guard = Guard(Lock_);
                KnownFileSnapshots_ = State_->KnownFileSnapshots;
                auto activeFileSnapshot = FindKnownFileSnapshot(State_->ActiveFileSnapshotId);
                auto preparingFileSnapshot = FindKnownFileSnapshot(State_->PreparingFileSnapshotId);
                if (staticSpecMatches && ArePersistedFileSnapshotsCompatible(activeFileSnapshot, preparingFileSnapshot)) {
                    ActiveFileSnapshot_ = std::move(activeFileSnapshot);
                    PreparingFileSnapshot_ = discoverySpecMatches ? std::move(preparingFileSnapshot) : nullptr;
                    LastFileSnapshotCreationTime_ = discoverySpecMatches
                        ? State_->LastFileSnapshotCreationTime
                        : std::nullopt;
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
            }
        }

        if (DiscoveryExecutor_) {
            DiscoveryExecutor_->Start();
        } else {
            DoDiscovery();
        }
    }

    TFuture<void> GetPreparingFuture() const
    {
        auto guard = Guard(Lock_);
        return PreparingPromise_.ToFuture().ToUncancelable();
    }

    void Reconfigure(const TDynamicResourceControllerContextPtr& dynamicContext)
    {
        for (const auto& [fileProviderId, provider] : Providers_) {
            auto providerContext = New<TDynamicFileProviderContext>();
            providerContext->DynamicFileProviderSpec = GetDynamicFileProviderSpec(
                dynamicContext->DynamicResourceSpec,
                fileProviderId);
            provider->Reconfigure(providerContext);
        }
        if (DiscoveryExecutor_) {
            DiscoveryExecutor_->SetPeriod(dynamicContext->DynamicResourceSpec->FileProviderDiscoverPeriod);
        }

        auto dynamicSpec = ConvertToNode(dynamicContext->DynamicResourceSpec)->AsMap();
        TPromise<void> previousPromise;
        {
            auto guard = Guard(Lock_);
            ++Generation_;
            DynamicResourceSpec_ = std::move(dynamicSpec);
            DiscoveryResult_.clear();
            previousPromise = std::exchange(PreparingPromise_, NewPromise<void>());
        }
        PreparingFileSnapshot_.Reset();
        LastFileSnapshotCreationTime_.reset();
        FileSnapshotMinCreationPeriod_ = dynamicContext->DynamicResourceSpec->FileSnapshotMinCreationPeriod;
        FileSnapshotCatalogMaxEntries_ = dynamicContext->DynamicResourceSpec->FileSnapshotCatalogMaxEntries;
        FileSnapshotRolloutWarningPeriod_ = dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod;
        PruneKnownFileSnapshots();

        previousPromise.TrySet(TError("Resource discovery was reconfigured"));
        if (Providers_.empty()) {
            DoDiscovery();
        }
    }

    std::pair<TFileSnapshotPtr, TFileSnapshotPtr> BuildTargetFileSnapshots()
    {
        const auto& Logger = Context_->Logger;
        THashMap<TFileProviderId, TFileProviderRevisionPtr> revisions;
        THashSet<TFileProviderId> pinnedProviders;
        {
            auto guard = Guard(Lock_);
            THROW_ERROR_EXCEPTION_UNLESS(DiscoveryResult_.size() == Providers_.size(), "Waiting for file provider discovery");
            for (const auto& [fileProviderId, result] : DiscoveryResult_) {
                revisions[fileProviderId] = result.Revision;
                if (result.Pinned) {
                    pinnedProviders.insert(fileProviderId);
                }
            }
        }
        PromotePinnedSnapshot(revisions, pinnedProviders);
        auto now = TInstant::Now();
        if (LastFileSnapshotCreationTime_ && *LastFileSnapshotCreationTime_ > now) {
            LastFileSnapshotCreationTime_ = now;
        }
        if (MatchesFileSnapshot(ActiveFileSnapshot_, revisions)) {
            PreparingFileSnapshot_.Reset();
        } else if (!Providers_.empty() &&
            !MatchesFileSnapshot(PreparingFileSnapshot_, revisions) &&
            (!LastFileSnapshotCreationTime_ ||
                now >= *LastFileSnapshotCreationTime_ + FileSnapshotMinCreationPeriod_))
        {
            auto snapshot = New<TFileSnapshot>();
            snapshot->Id = TFileSnapshotId(Context_->TimeProvider->GenerateSeqNo());
            snapshot->FileProviders = std::move(revisions);
            PreparingFileSnapshot_ = std::move(snapshot);
            RegisterKnownFileSnapshot(PreparingFileSnapshot_);
            LastFileSnapshotCreationTime_ = now;
            YT_TLOG_INFO("Created file snapshot for rollout")
                .With("FileSnapshotId", PreparingFileSnapshot_->Id);
        }
        if (State_.IsInitialized()) {
            State_->ResourceSpec = ConvertToNode(Context_->ResourceSpec)->AsMap();
            State_->DynamicResourceSpec = DynamicResourceSpec_;
        }
        PersistFileSnapshotState();
        UpdateTargetMetrics();
        return {ActiveFileSnapshot_, PreparingFileSnapshot_};
    }

    void PromotePinnedSnapshot(
        const THashMap<TFileProviderId, TFileProviderRevisionPtr>& discoveredRevisions,
        const THashSet<TFileProviderId>& pinnedProviders)
    {
        if (Providers_.empty() || (!ForceActive_ && pinnedProviders.empty())) {
            return;
        }
        auto revisions = ActiveFileSnapshot_ ? ActiveFileSnapshot_->FileProviders : discoveredRevisions;
        for (const auto& fileProviderId : pinnedProviders) {
            revisions[fileProviderId] = GetOrCrash(discoveredRevisions, fileProviderId);
        }
        if (MatchesFileSnapshot(ActiveFileSnapshot_, revisions)) {
            return;
        }
        auto snapshot = New<TFileSnapshot>();
        snapshot->Id = TFileSnapshotId(Context_->TimeProvider->GenerateSeqNo());
        snapshot->FileProviders = std::move(revisions);
        ActiveFileSnapshot_ = std::move(snapshot);
        ForceActive_ = false;
        PreparingFileSnapshot_.Reset();
        ActiveFileSnapshotPublishedAt_ = TInstant::Now();
        RegisterKnownFileSnapshot(ActiveFileSnapshot_);
    }

    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        std::optional<i64> publishedRevisionId)
    {
        const auto& Logger = Context_->Logger;
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

        std::optional<TFileSnapshotId> promotedSnapshotId;
        if (publishedRevisionId) {
            auto guard = Guard(Lock_);
            if (PreparingFileSnapshot_) {
                for (const auto& [_, status] : currentTargetWorkerStatuses) {
                    if (status->PreparingFileSnapshot &&
                        status->PreparingFileSnapshot->SnapshotId == PreparingFileSnapshot_->Id &&
                        status->PreparingFileSnapshot->State == EFileSnapshotState::Validated)
                    {
                        ActiveFileSnapshot_ = PreparingFileSnapshot_;
                        promotedSnapshotId = ActiveFileSnapshot_->Id;
                        PreparingFileSnapshot_.Reset();
                        ActiveFileSnapshotPublishedAt_ = TInstant::Now();
                        PersistFileSnapshotState();
                        break;
                    }
                }
            }
        }

        if (promotedSnapshotId) {
            YT_TLOG_INFO("Promoted file snapshot to active target")
                .With("FileSnapshotId", *promotedSnapshotId);
        }

        UpdateRolloutStatus(authoritativeWorkerStatuses, publishedRevisionId);

        auto guard = Guard(Lock_);
        UpdateTargetMetrics();
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
            for (const auto& [fileProviderId, result] : DiscoveryResult_) {
                view->ProviderRevisions[fileProviderId] = result.Revision;
            }
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

    void UpdateTargetMetrics()
    {
        THashSet<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, std::string, std::string>> present;
        const auto now = TInstant::Now();
        auto update = [&] (const TFileSnapshotPtr& snapshot, const std::string& state) {
            if (!snapshot) {
                return;
            }
            for (const auto& [id, revision] : snapshot->FileProviders) {
                auto key = std::tuple(id, revision->ObjectId, state, revision->DisplayVersion);
                present.insert(key);
                auto profiler = Context_->Profiler
                    .WithTag("file_provider_id", id.Underlying())
                    .WithTag("revision_id", revision->ObjectId.Underlying())
                    .WithTag("state", state)
                    .WithTag("display_version", revision->DisplayVersion);
                auto [it, inserted] = TargetRevisionGauges_.emplace(key, TGauge{});
                if (inserted) {
                    it->second = profiler.Gauge("/file_provider_target_revision");
                }
                it->second.Update(1);
                if (revision->Timestamp) {
                    auto [ageIt, ageInserted] = TargetRevisionAgeGauges_.emplace(key, TGauge{});
                    if (ageInserted) {
                        ageIt->second = profiler.Gauge("/file_provider_target_revision_age");
                    }
                    ageIt->second.Update(now.SecondsFloat() - revision->Timestamp->SecondsFloat());
                } else {
                    TargetRevisionAgeGauges_.erase(key);
                }
            }
        };
        update(ActiveFileSnapshot_, "active");
        update(PreparingFileSnapshot_, "preparing");
        for (auto it = TargetRevisionGauges_.begin(); it != TargetRevisionGauges_.end();) {
            if (!present.contains(it->first)) {
                it->second.Update(0);
                TargetRevisionAgeGauges_.erase(it->first);
                auto toErase = it++;
                TargetRevisionGauges_.erase(toErase);
            } else {
                ++it;
            }
        }
    }

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
        const TFileProviderRevisionPtr& lhs,
        const TFileProviderRevisionPtr& rhs)
    {
        auto lhsNode = ConvertToNode(lhs)->AsMap();
        auto rhsNode = ConvertToNode(rhs)->AsMap();
        lhsNode->RemoveChild("timestamp");
        rhsNode->RemoveChild("timestamp");
        return AreNodesEqual(lhsNode, rhsNode);
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
                !AreFileProviderRevisionsEqual(revision, it->second))
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
        for (const auto& [fileProviderId, spec] : Context_->ResourceSpec->FileProviders) {
            auto it = snapshot->FileProviders.find(fileProviderId);
            if (it == snapshot->FileProviders.end() ||
                !it->second ||
                it->second->FileProviderClassName != spec->FileProviderClassName)
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

    void DoDiscovery()
    {
        auto generation = [this] {
            auto guard = Guard(Lock_);
            return Generation_;
        }();

        THashMap<TFileProviderId, TFuture<TFileProviderDiscoveryResult>> futures;
        for (const auto& [fileProviderId, provider] : Providers_) {
            futures[fileProviderId] = provider->Discover();
        }

        THashMap<TFileProviderId, TFileProviderDiscoveryResult> results;
        for (const auto& [fileProviderId, future] : futures) {
            auto result = WaitFor(future)
                .ValueOrThrow();
            THROW_ERROR_EXCEPTION_UNLESS(result.Revision, "File provider %Qv discovery returned no revision", fileProviderId);
            const auto& expectedClassName = GetOrCrash(Context_->ResourceSpec->FileProviders, fileProviderId)->FileProviderClassName;
            THROW_ERROR_EXCEPTION_UNLESS(result.Revision->FileProviderClassName == expectedClassName, "Discovered file provider %Qv class %Qv differs from configured class %Qv", fileProviderId, result.Revision->FileProviderClassName, expectedClassName);
            results[fileProviderId] = std::move(result);
        }

        auto promise = [&] {
            auto guard = Guard(Lock_);
            THROW_ERROR_EXCEPTION_UNLESS(generation == Generation_, "Resource discovery was reconfigured");
            const auto& Logger = Context_->Logger;
            for (const auto& [fileProviderId, result] : results) {
                auto it = DiscoveryResult_.find(fileProviderId);
                if (it == DiscoveryResult_.end() || !AreFileProviderRevisionsEqual(it->second.Revision, result.Revision)) {
                    YT_TLOG_INFO("Discovered new file provider revision")
                        .With("FileProvider", fileProviderId)
                        .With("ObjectId", result.Revision->ObjectId)
                        .With("DisplayVersion", result.Revision->DisplayVersion)
                        .With("ExpectedSize", result.Revision->Size);
                }
            }
            DiscoveryResult_ = std::move(results);
            return PreparingPromise_;
        }();
        promise.TrySet();
    }

    void Discover()
    {
        const auto& Logger = Context_->Logger;
        try {
            DoDiscovery();
            DiscoveryError_->ClearError();
        } catch (const std::exception& ex) {
            auto error = TError("File provider discovery failed")
                .With(TError(ex));
            DiscoveryError_->SetError(error);
            YT_TLOG_WARNING("File provider discovery failed")
                .With(error);
        }
    }

    const TResourceControllerContextPtr Context_;
    THashMap<TFileProviderId, IFileProviderPtr> Providers_;
    TPeriodicExecutorPtr DiscoveryExecutor_;
    IStatusErrorStatePtr DiscoveryError_;

    mutable TSpinLock Lock_;
    TMutableStateClient<TFileProviderDiscoveryState> State_;
    THashMap<TFileProviderId, TFileProviderDiscoveryResult> DiscoveryResult_;
    IMapNodePtr DynamicResourceSpec_;
    i64 Generation_ = 0;
    TPromise<void> PreparingPromise_ = NewPromise<void>();
    bool ForceActive_ = false;
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
    THashMap<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, std::string, std::string>, TGauge> TargetRevisionGauges_;
    THashMap<std::tuple<TFileProviderId, NFileStorage::TFileStorageObjectId, std::string, std::string>, TGauge> TargetRevisionAgeGauges_;
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
    : Context_([&] {
        if (context->Invoker) {
            context->Invoker = CancelableContext_->CreateInvoker(context->Invoker);
        }
        return std::move(context);
    }())
    , DynamicContext_(dynamicContext)
    , Parameters_(TRegistry::Get()->ParseResourceParameters(Context_->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(
        Context_->ResourceSpec,
        dynamicContext->DynamicResourceSpec))
    , FileProviderDiscovery_(New<TFileProviderDiscovery>(Context_, dynamicContext))
    , Logger(Context_->Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicResourceControllerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(
            Context_->ResourceSpec,
            dynamicContext->DynamicResourceSpec);
        FileProviderDiscovery_->Reconfigure(dynamicContext);
        TargetRevision_.Reset();
    }));
}

void TResourceControllerBase::Init(IInitContextPtr initContext)
{
    DoInit(initContext ? initContext->WithPrefix("controller") : nullptr);
    FileProviderDiscovery_->Init(initContext ? initContext->WithPrefix("file_providers") : nullptr);
}

TFuture<void> TResourceControllerBase::GetPreparingFuture()
{
    return FileProviderDiscovery_->GetPreparingFuture();
}

TResourceRevisionPtr TResourceControllerBase::BuildTargetRevision()
{
    auto [active, preparing] = FileProviderDiscovery_->BuildTargetFileSnapshots();
    auto spec = DoBuildTargetRevisionSpec();
    if (!spec && !active && !preparing) {
        TargetRevision_.Reset();
        return nullptr;
    }
    auto revision = New<TResourceRevision>();
    revision->Spec = spec ? CloneNode(spec) : nullptr;
    revision->ActiveFileSnapshot = std::move(active);
    revision->PreparingFileSnapshot = std::move(preparing);
    revision->RevisionId = TargetRevision_ ? TargetRevision_->RevisionId : 0;
    if (!TargetRevision_ || !AreNodesEqual(ConvertToNode(revision), ConvertToNode(TargetRevision_))) {
        revision->RevisionId = Context_->VersionProvider->GenerateVersion().Underlying();
        TargetRevision_ = std::move(revision);
    }
    return TargetRevision_;
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

const TCancelableContextPtr& TResourceControllerBase::GetCancelableContext() const
{
    return CancelableContext_;
}

void TResourceControllerBase::OnReconfigureFail(const TError& error)
{
    Stop(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
