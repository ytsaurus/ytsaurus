#pragma once

#ifndef FILE_RESOURCE_INL_H_
    #error "Direct inclusion of this file is not allowed, include file_resource.h"
#endif

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TData>
TFileResourceAccessor<TData>::TFileResourceAccessor(
    TIntrusivePtr<TSnapshot> snapshot)
    : Snapshot_(std::move(snapshot))
{
    YT_VERIFY(Snapshot_);
    Snapshot_->AccessorState->LiveAccessorCount.fetch_add(1, std::memory_order::relaxed);
}

template <class TData>
TFileResourceAccessor<TData>::TFileResourceAccessor(const TFileResourceAccessor& other)
    : Snapshot_(other.Snapshot_)
{
    if (Snapshot_) {
        Snapshot_->AccessorState->LiveAccessorCount.fetch_add(1, std::memory_order::relaxed);
    }
}

template <class TData>
TFileResourceAccessor<TData>::TFileResourceAccessor(TFileResourceAccessor&& other) noexcept
    : Snapshot_(std::move(other.Snapshot_))
{ }

template <class TData>
TFileResourceAccessor<TData>::~TFileResourceAccessor()
{
    Release();
}

template <class TData>
TFileResourceAccessor<TData>& TFileResourceAccessor<TData>::operator=(const TFileResourceAccessor& other)
{
    if (this != &other) {
        auto snapshot = other.Snapshot_;
        if (snapshot) {
            snapshot->AccessorState->LiveAccessorCount.fetch_add(1, std::memory_order::relaxed);
        }
        Release();
        Snapshot_ = std::move(snapshot);
    }
    return *this;
}

template <class TData>
TFileResourceAccessor<TData>& TFileResourceAccessor<TData>::operator=(TFileResourceAccessor&& other) noexcept
{
    if (this != &other) {
        Release();
        Snapshot_ = std::move(other.Snapshot_);
    }
    return *this;
}

template <class TData>
void TFileResourceAccessor<TData>::Release()
{
    if (Snapshot_) {
        auto previous = Snapshot_->AccessorState->LiveAccessorCount.fetch_sub(1, std::memory_order::relaxed);
        YT_VERIFY(previous > 0);
        Snapshot_.Reset();
    }
}

template <class TData>
const TData& TFileResourceAccessor<TData>::operator*() const
{
    return *Snapshot_->Data;
}

template <class TData>
const TData* TFileResourceAccessor<TData>::operator->() const
{
    return Snapshot_->Data.Get();
}

template <class TData>
const TFileSourceRevisionPtr& TFileResourceAccessor<TData>::GetSourceRevision(const TFileSourceId& id) const
{
    return Snapshot_->FileSources->GetFileSource(id)->GetRevision();
}

template <class TData>
const std::string& TFileResourceAccessor<TData>::GetRootPath(const TFileSourceId& id) const
{
    return Snapshot_->FileSources->GetFileSource(id)->GetRootPath();
}

template <class TData>
const TMaterializedFileSourceSnapshotPtr& TFileResourceAccessor<TData>::GetFileSources() const
{
    return Snapshot_->FileSources;
}

template <class TData>
TFileSnapshotId TFileResourceAccessor<TData>::GetFileSnapshotId() const
{
    return Snapshot_->FileSources->GetFileSnapshot()->Id;
}

template <class TData>
i64 TFileResourceAccessor<TData>::GetDeliveryRevisionId() const
{
    return Snapshot_->DeliveryRevision->RevisionId;
}

////////////////////////////////////////////////////////////////////////////////

template <class TData>
TFileResourceBase<TData>::TFileResourceBase(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(std::move(context), dynamicContext)
    , UpdateError_(GetContext()->StatusProfiler
            ? GetContext()->StatusProfiler->ErrorState("/file_update")
            : nullptr)
    , ActivationError_(GetContext()->StatusProfiler
            ? GetContext()->StatusProfiler->ErrorState("/file_snapshot_activation")
            : nullptr)
    , InitializationInvoker_(NConcurrency::CreateSerializedInvoker(
        GetContext()->Invoker,
        "FileResourceInitialization"))
    , WeakThis_(MakeWeak(this))
    , UpdateRetryPeriod_(dynamicContext->DynamicResourceSpec->FileSourceUpdateRetryPeriod)
    , ActivationStallWarningPeriod_(dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod)
{
    SubscribeReconfigured(BIND(&TFileResourceBase::OnReconfigured, Unretained(this)));
    OnReconfigured(dynamicContext);
}

template <class TData>
TFuture<void> TFileResourceBase<TData>::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    auto guard = Guard(Lock_);
    LoadStarted_ = true;
    SchedulePreparation(guard);
    return InitialLoadPromise_.ToFuture().ToUncancelable();
}

template <class TData>
TResourceRevisionState TFileResourceBase<TData>::GetRevisionState() const
{
    auto guard = Guard(Lock_);
    THashMap<TFileSnapshotId, i64> liveAccessorCounts;
    auto collectAccessorCount = [&] (
        TFileSnapshotId snapshotId,
        const TIntrusivePtr<NDetail::TFileResourceAccessorState>& accessorState) {
        auto count = accessorState->LiveAccessorCount.load(std::memory_order::relaxed);
        if (count > 0) {
            liveAccessorCounts[snapshotId] += count;
        }
    };
    if (auto activeSnapshot = ActiveSnapshot_.Acquire()) {
        collectAccessorCount(
            activeSnapshot->FileSources->GetFileSnapshot()->Id,
            activeSnapshot->AccessorState);
    }
    if (ActivationState_ &&
        ActivationState_->PreviousSnapshotId &&
        ActivationState_->PreviousAccessorState)
    {
        collectAccessorCount(
            *ActivationState_->PreviousSnapshotId,
            ActivationState_->PreviousAccessorState);
    }
    const auto* reportedSlot = &PreparingSlot_;
    if (ActiveSlot_.Desired &&
        ActiveSlot_.State != EFileSnapshotState::Active &&
        (!PreparingSlot_.Desired ||
            PreparingSlot_.AttemptGeneration == 0 ||
            ActiveSlot_.InFlight ||
            ActiveSlot_.State == EFileSnapshotState::Draining))
    {
        reportedSlot = &ActiveSlot_;
    }
    auto preparingFileSnapshot = BuildSnapshotStatus(*reportedSlot);
    return {
        .AppliedRevisionId = AppliedRevision_ ? std::optional(AppliedRevision_->RevisionId) : std::nullopt,
        .TargetRevisionId = Target_ ? std::optional(Target_->RevisionId) : std::nullopt,
        .ActiveFileSnapshotId = ActiveFileSnapshotId_,
        .PreparingFileSnapshot = std::move(preparingFileSnapshot),
        .LiveAccessorCounts = std::move(liveAccessorCounts),
        .ResourceInstanceId = GetContext()->ResourceInstanceId,
        .ResourceIncarnationGeneration = GetContext()->ResourceIncarnationGeneration,
    };
}

template <class TData>
typename TFileResourceBase<TData>::TAccessor TFileResourceBase<TData>::Lock() const
{
    auto snapshot = ActiveSnapshot_.Acquire();
    THROW_ERROR_EXCEPTION_UNLESS(snapshot, "File resource has no initialized data");
    return TAccessor(std::move(snapshot));
}

template <class TData>
void TFileResourceBase<TData>::Validate(const TDataPtr& /*data*/)
{ }

template <class TData>
void TFileResourceBase<TData>::OnReconfigured(const TDynamicResourceContextPtr& dynamicContext)
{
    TSnapshotPtr discardedAppliedSnapshot;
    TSnapshotPtr discardedActiveCandidate;
    TSnapshotPtr discardedPreparingCandidate;
    {
        auto guard = Guard(Lock_);

        bool retryPeriodChanged =
            UpdateRetryPeriod_ != dynamicContext->DynamicResourceSpec->FileSourceUpdateRetryPeriod;
        UpdateRetryPeriod_ = dynamicContext->DynamicResourceSpec->FileSourceUpdateRetryPeriod;
        ActivationStallWarningPeriod_ =
            dynamicContext->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod;
        Target_ = dynamicContext->TargetRevision;
        auto desiredActive = Target_ ? Target_->ActiveFileSnapshot : nullptr;
        auto desiredPreparing = Target_ ? Target_->PreparingFileSnapshot : nullptr;
        auto matches = [] (const TSnapshotSlot& slot, const TFileSnapshotPtr& desired) {
            return slot.Desired && desired && slot.Desired->Id == desired->Id;
        };
        auto needsPreparing = desiredPreparing &&
            (!desiredActive || desiredPreparing->Id != desiredActive->Id);

        if (ActivationState_) {
            YT_VERIFY(ActiveSlot_.Desired);
            if (matches(ActiveSlot_, desiredActive)) {
                ActiveSlot_.Desired = desiredActive;
                ActivationState_->DeliveryRevision = Target_;
                auto appliedSnapshot = ActiveSnapshot_.Acquire();
                YT_VERIFY(appliedSnapshot);
                discardedAppliedSnapshot = ActiveSnapshot_.Exchange(New<TSnapshot>(
                    appliedSnapshot->Data,
                    appliedSnapshot->FileSources,
                    Target_,
                    appliedSnapshot->AccessorState,
                    appliedSnapshot->LifetimeAnchor));
            }

            if (needsPreparing && matches(PreparingSlot_, desiredPreparing)) {
                PreparingSlot_.Desired = desiredPreparing;
                if (retryPeriodChanged) {
                    PreparingSlot_.NextRetryAt.reset();
                }
            } else {
                discardedPreparingCandidate = std::move(PreparingSlot_.Candidate);
                PreparingSlot_ = {};
                if (needsPreparing) {
                    PreparingSlot_.Desired = desiredPreparing;
                }
            }
            return;
        }

        auto previousActiveSlot = std::move(ActiveSlot_);
        auto previousPreparingSlot = std::move(PreparingSlot_);
        auto appliedSnapshot = ActiveSnapshot_.Acquire();
        bool appliedSnapshotMatches = desiredActive &&
            appliedSnapshot &&
            ActiveFileSnapshotId_ == desiredActive->Id &&
            appliedSnapshot->FileSources->GetFileSnapshot()->Id == desiredActive->Id;
        bool reuseActiveSlot = desiredActive &&
            !appliedSnapshotMatches &&
            matches(previousActiveSlot, desiredActive);
        bool promotePreparingSlot = desiredActive &&
            !appliedSnapshotMatches &&
            !reuseActiveSlot &&
            matches(previousPreparingSlot, desiredActive) &&
            previousPreparingSlot.State == EFileSnapshotState::Validated &&
            previousPreparingSlot.Candidate;
        bool reusePreparingSlot = needsPreparing &&
            !promotePreparingSlot &&
            matches(previousPreparingSlot, desiredPreparing);
        discardedActiveCandidate = std::move(previousActiveSlot.Candidate);
        discardedPreparingCandidate = std::move(previousPreparingSlot.Candidate);

        ActiveSlot_ = {};
        PreparingSlot_ = {};

        if (desiredActive) {
            ActiveSlot_.Desired = desiredActive;
            if (appliedSnapshotMatches) {
                ActiveSlot_.State = EFileSnapshotState::Active;
                AppliedRevision_ = Target_;
                discardedAppliedSnapshot = ActiveSnapshot_.Exchange(New<TSnapshot>(
                    appliedSnapshot->Data,
                    appliedSnapshot->FileSources,
                    Target_,
                    appliedSnapshot->AccessorState,
                    appliedSnapshot->LifetimeAnchor));
            } else if (reuseActiveSlot) {
                ActiveSlot_ = std::move(previousActiveSlot);
                ActiveSlot_.Desired = desiredActive;
                ActiveSlot_.Candidate = std::move(discardedActiveCandidate);
                if (retryPeriodChanged) {
                    ActiveSlot_.NextRetryAt.reset();
                }
            }
        }

        if (promotePreparingSlot) {
            ActiveSlot_ = std::move(previousPreparingSlot);
            ActiveSlot_.Desired = desiredActive;
            ActiveSlot_.Candidate = std::move(discardedPreparingCandidate);
            if (needsPreparing) {
                PreparingSlot_.Desired = desiredPreparing;
            }
        } else if (reusePreparingSlot) {
            PreparingSlot_ = std::move(previousPreparingSlot);
            PreparingSlot_.Desired = desiredPreparing;
            PreparingSlot_.Candidate = std::move(discardedPreparingCandidate);
            if (retryPeriodChanged) {
                PreparingSlot_.NextRetryAt.reset();
            }
        } else if (needsPreparing) {
            PreparingSlot_.Desired = desiredPreparing;
        }

        SchedulePreparation(guard);
    }
}

template <class TData>
void TFileResourceBase<TData>::SchedulePreparation(const TGuard<NThreading::TSpinLock>& guard)
{
    if (!LoadStarted_ || !Target_) {
        return;
    }

    if (ActiveSlot_.InFlight || PreparingSlot_.InFlight) {
        return;
    }

    if (ActiveSlot_.Desired &&
        ActiveSlot_.State != EFileSnapshotState::Active &&
        !ActiveSlot_.NextRetryAt)
    {
        StartPreparation(ESnapshotRole::Active, guard);
        return;
    }

    if (PreparingSlot_.Desired &&
        PreparingSlot_.State != EFileSnapshotState::Validated &&
        !PreparingSlot_.NextRetryAt)
    {
        StartPreparation(ESnapshotRole::Preparing, guard);
    }
}

template <class TData>
void TFileResourceBase<TData>::StartPreparation(
    ESnapshotRole role,
    const TGuard<NThreading::TSpinLock>& /*guard*/)
{
    auto& slot = GetSlot(role);
    YT_VERIFY(slot.Desired);
    YT_VERIFY(!slot.InFlight);

    auto target = Target_;
    auto fileSnapshot = slot.Desired;
    const bool activateValidatedCandidate =
        role == ESnapshotRole::Active &&
        slot.State == EFileSnapshotState::Validated &&
        slot.Candidate;
    auto attemptGeneration = activateValidatedCandidate && slot.AttemptGeneration != 0
        ? slot.AttemptGeneration
        : ++NextAttemptGeneration_;
    slot.AttemptGeneration = attemptGeneration;
    slot.InFlight = true;
    slot.Error = {};
    slot.NextRetryAt.reset();

    if (activateValidatedCandidate) {
        slot.PreparationStage.reset();
        GetContext()->Invoker->Invoke(BIND([
            weakThis = MakeWeak(this),
            target = std::move(target),
            fileSnapshot = std::move(fileSnapshot),
            attemptGeneration
        ] {
            if (auto strongThis = weakThis.Lock()) {
                strongThis->ActivateCandidate(target, fileSnapshot, attemptGeneration);
            }
        }));
        return;
    }

    slot.State = EFileSnapshotState::Preparing;
    slot.PreparationStage = EFileSnapshotPreparationStage::Materializing;
    GetContext()->Invoker->Invoke(BIND([
        weakThis = MakeWeak(this),
        role,
        target = std::move(target),
        fileSnapshot = std::move(fileSnapshot),
        attemptGeneration
    ] {
        if (auto strongThis = weakThis.Lock()) {
            strongThis->BeginPreparation(
                role,
                target,
                fileSnapshot,
                attemptGeneration);
        }
    }));
}

template <class TData>
void TFileResourceBase<TData>::BeginPreparation(
    ESnapshotRole role,
    TResourceRevisionPtr target,
    TFileSnapshotPtr fileSnapshot,
    ui64 attemptGeneration)
{
    {
        auto guard = Guard(Lock_);
        if (!IsCurrentAttempt(role, target, fileSnapshot, attemptGeneration) ||
            !GetSlot(role).InFlight)
        {
            return;
        }
    }

    try {
        ValidateSnapshot(fileSnapshot);

        MaterializeFileSources(fileSnapshot).Subscribe(BIND([weakThis = MakeWeak(this), role, target = std::move(target), fileSnapshot = std::move(fileSnapshot), attemptGeneration] (const TErrorOr<TMaterializedFileSourceSnapshotPtr>& result) {
            if (auto strongThis = weakThis.Lock()) {
                strongThis->OnMaterialized(
                    role,
                    target,
                    fileSnapshot,
                    attemptGeneration,
                    result);
            }
        }).Via(InitializationInvoker_));
    } catch (const std::exception& ex) {
        HandlePreparationError(
            role,
            target,
            fileSnapshot,
            attemptGeneration,
            TError(ex));
    }
}

template <class TData>
void TFileResourceBase<TData>::OnMaterialized(
    ESnapshotRole role,
    const TResourceRevisionPtr& target,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration,
    const TErrorOr<TMaterializedFileSourceSnapshotPtr>& result)
{
    try {
        auto fileSources = result.ValueOrThrow();
        if (!SetSnapshotPreparationStage(
            role,
            target,
            fileSnapshot,
            attemptGeneration,
            EFileSnapshotPreparationStage::Initializing))
        {
            return;
        }

        auto data = Initialize(fileSources);
        THROW_ERROR_EXCEPTION_UNLESS(data, "File resource initializer returned null data");
        if (!SetSnapshotPreparationStage(
            role,
            target,
            fileSnapshot,
            attemptGeneration,
            EFileSnapshotPreparationStage::Validating))
        {
            return;
        }

        Validate(data);
        CompletePreparation(
            role,
            target,
            fileSnapshot,
            attemptGeneration,
            New<TSnapshot>(std::move(data), std::move(fileSources), target));
    } catch (const std::exception& ex) {
        HandlePreparationError(
            role,
            target,
            fileSnapshot,
            attemptGeneration,
            TError(ex));
    }
}

template <class TData>
void TFileResourceBase<TData>::ValidateSnapshot(
    const TFileSnapshotPtr& fileSnapshot) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        fileSnapshot->FileSources.size() == GetSpec()->FileSources.size(),
        "File snapshot %v has %v file sources while the spec configures %v",
        fileSnapshot->Id,
        fileSnapshot->FileSources.size(),
        GetSpec()->FileSources.size());

    for (const auto& [name, sourceSpec] : GetSpec()->FileSources) {
        auto revisionIt = fileSnapshot->FileSources.find(name);
        THROW_ERROR_EXCEPTION_UNLESS(
            revisionIt != fileSnapshot->FileSources.end(),
            "File snapshot %v has no file source %Qv",
            fileSnapshot->Id,
            name);
        THROW_ERROR_EXCEPTION_UNLESS(
            revisionIt->second,
            "File snapshot %v has null file source %Qv",
            fileSnapshot->Id,
            name);
        THROW_ERROR_EXCEPTION_UNLESS(
            revisionIt->second->FileSourceClassName == sourceSpec->FileSourceClassName,
            "File snapshot source %Qv class %Qv differs from configured class %Qv",
            name,
            revisionIt->second->FileSourceClassName,
            sourceSpec->FileSourceClassName);
    }
}

template <class TData>
void TFileResourceBase<TData>::CompletePreparation(
    ESnapshotRole role,
    const TResourceRevisionPtr& target,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration,
    TSnapshotPtr candidate)
{
    {
        auto guard = Guard(Lock_);
        if (!IsCurrentAttempt(role, target, fileSnapshot, attemptGeneration) ||
            !GetSlot(role).InFlight)
        {
            return;
        }

        auto& slot = GetSlot(role);
        slot.InFlight = false;
        slot.State = EFileSnapshotState::Validated;
        slot.PreparationStage.reset();
        slot.Error = {};
        slot.NextRetryAt.reset();
        slot.Candidate = std::move(candidate);
        SchedulePreparation(guard);
    }

    if (UpdateError_) {
        UpdateError_->ClearError();
    }
}

template <class TData>
void TFileResourceBase<TData>::ActivateCandidate(
    const TResourceRevisionPtr& target,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration)
{
    bool completeInitialLoad = false;
    TSnapshotPtr candidate;
    TSnapshotPtr previousSnapshot;
    {
        auto guard = Guard(Lock_);
        if (!IsCurrentAttempt(
            ESnapshotRole::Active,
            target,
            fileSnapshot,
            attemptGeneration) ||
            !ActiveSlot_.InFlight ||
            ActiveSlot_.State != EFileSnapshotState::Validated ||
            !ActiveSlot_.Candidate)
        {
            return;
        }

        YT_VERIFY(Target_);
        candidate = std::move(ActiveSlot_.Candidate);
        candidate->DeliveryRevision = Target_;
        previousSnapshot = ActiveSnapshot_.Exchange(candidate);
        ActiveSlot_.State = EFileSnapshotState::Draining;

        std::optional<TFileSnapshotId> previousSnapshotId;
        if (previousSnapshot) {
            previousSnapshotId = previousSnapshot->FileSources->GetFileSnapshot()->Id;
        }
        ActivationState_ = TActivationState{
            .PreviousSnapshotId = previousSnapshotId,
            .PreviousAccessorState = previousSnapshot
                ? previousSnapshot->AccessorState
                : nullptr,
            .DeliveryRevision = Target_,
            .AttemptGeneration = attemptGeneration,
            .RetiredSnapshotReleased = !previousSnapshot,
        };

        if (previousSnapshot) {
            auto retirementCallback = New<NDetail::TFileResourceLifetimeCallback>(BIND([
                weakThis = WeakThis_,
                invoker = GetContext()->Invoker,
                previousSnapshotId = *previousSnapshotId,
                attemptGeneration
            ] {
                invoker->Invoke(BIND([weakThis, previousSnapshotId, attemptGeneration] {
                    if (auto strongThis = weakThis.Lock()) {
                        strongThis->OnRetiredSnapshotReleased(
                            previousSnapshotId,
                            attemptGeneration);
                    }
                }));
            }));
            YT_VERIFY(!previousSnapshot->LifetimeAnchor->RetirementCallback);
            previousSnapshot->LifetimeAnchor->RetirementCallback = retirementCallback;
            retirementCallback->Arm();

            auto warningPeriod = ActivationStallWarningPeriod_;
            NConcurrency::TDelayedExecutor::Submit(
                BIND([
                    weakThis = WeakThis_,
                    previousSnapshotId = *previousSnapshotId,
                    attemptGeneration,
                    warningPeriod
                ] {
                    if (auto strongThis = weakThis.Lock()) {
                        strongThis->ReportActivationStall(
                            previousSnapshotId,
                            attemptGeneration,
                            warningPeriod);
                    }
                }),
                warningPeriod,
                GetContext()->Invoker);
        }

        completeInitialLoad = TryCompleteActivation(guard);
    }
    previousSnapshot.Reset();

    if (UpdateError_) {
        UpdateError_->ClearError();
    }
    if (completeInitialLoad) {
        InitialLoadPromise_.Set();
    }
}

template <class TData>
void TFileResourceBase<TData>::OnRetiredSnapshotReleased(
    TFileSnapshotId previousSnapshotId,
    ui64 attemptGeneration)
{
    bool completeInitialLoad = false;
    {
        auto guard = Guard(Lock_);
        if (!ActivationState_ ||
            ActivationState_->AttemptGeneration != attemptGeneration ||
            ActivationState_->PreviousSnapshotId != previousSnapshotId)
        {
            return;
        }
        ActivationState_->RetiredSnapshotReleased = true;
        completeInitialLoad = TryCompleteActivation(guard);
    }
    if (completeInitialLoad) {
        InitialLoadPromise_.Set();
    }
}

template <class TData>
void TFileResourceBase<TData>::ReportActivationStall(
    TFileSnapshotId previousSnapshotId,
    ui64 attemptGeneration,
    TDuration warningPeriod)
{
    auto guard = Guard(Lock_);
    if (!ActivationState_ ||
        ActivationState_->AttemptGeneration != attemptGeneration ||
        ActivationState_->PreviousSnapshotId != previousSnapshotId ||
        ActivationState_->RetiredSnapshotReleased ||
        !ActivationState_->PreviousAccessorState ||
        !ActiveSlot_.Desired)
    {
        return;
    }

    auto liveAccessorCount = ActivationState_->PreviousAccessorState
        ->LiveAccessorCount.load(std::memory_order::relaxed);
    if (liveAccessorCount == 0 || !ActivationError_) {
        return;
    }

    ActivationError_->SetError(
        TError("File snapshot activation is blocked by live accessors")
            .With("previous_snapshot_id", previousSnapshotId)
            .With("activating_snapshot_id", ActiveSlot_.Desired->Id)
            .With("live_accessor_count", liveAccessorCount)
            .With("warning_period", warningPeriod));
}

template <class TData>
bool TFileResourceBase<TData>::TryCompleteActivation(
    const TGuard<NThreading::TSpinLock>& guard)
{
    if (!ActivationState_ || !ActivationState_->RetiredSnapshotReleased) {
        return false;
    }
    return CompleteActivation(guard);
}

template <class TData>
bool TFileResourceBase<TData>::CompleteActivation(
    const TGuard<NThreading::TSpinLock>& guard)
{
    YT_VERIFY(ActivationState_);
    YT_VERIFY(ActiveSlot_.Desired);
    YT_VERIFY(ActiveSlot_.InFlight);
    YT_VERIFY(ActiveSlot_.State == EFileSnapshotState::Draining);

    ActiveFileSnapshotId_ = ActiveSlot_.Desired->Id;
    AppliedRevision_ = ActivationState_->DeliveryRevision;
    ActiveSlot_.InFlight = false;
    ActiveSlot_.State = EFileSnapshotState::Active;
    ActiveSlot_.PreparationStage.reset();
    ActiveSlot_.Error = {};
    ActiveSlot_.NextRetryAt.reset();
    ActivationState_.reset();
    if (ActivationError_) {
        ActivationError_->ClearError();
    }

    bool completeInitialLoad = false;
    if (!InitialLoadCompleted_) {
        InitialLoadCompleted_ = true;
        completeInitialLoad = true;
    }

    ConfigureSlotsAfterActivation(guard);
    SchedulePreparation(guard);
    return completeInitialLoad;
}

template <class TData>
void TFileResourceBase<TData>::ConfigureSlotsAfterActivation(
    const TGuard<NThreading::TSpinLock>& /*guard*/)
{
    YT_VERIFY(ActiveFileSnapshotId_);

    ActiveSlot_ = {};
    PreparingSlot_ = {};
    if (!Target_) {
        return;
    }

    if (Target_->ActiveFileSnapshot) {
        ActiveSlot_.Desired = Target_->ActiveFileSnapshot;
        if (Target_->ActiveFileSnapshot->Id == *ActiveFileSnapshotId_) {
            ActiveSlot_.State = EFileSnapshotState::Active;
            AppliedRevision_ = Target_;
        }
    }
    if (Target_->PreparingFileSnapshot &&
        (!Target_->ActiveFileSnapshot ||
            Target_->PreparingFileSnapshot->Id != Target_->ActiveFileSnapshot->Id))
    {
        PreparingSlot_.Desired = Target_->PreparingFileSnapshot;
    }
}

template <class TData>
void TFileResourceBase<TData>::HandlePreparationError(
    ESnapshotRole role,
    const TResourceRevisionPtr& target,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration,
    const TError& error)
{
    TSnapshotPtr discardedCandidate;
    TError wrappedError;
    TDuration retryPeriod;
    {
        auto guard = Guard(Lock_);
        if (!IsCurrentAttempt(role, target, fileSnapshot, attemptGeneration) ||
            !GetSlot(role).InFlight)
        {
            return;
        }

        auto& slot = GetSlot(role);
        auto revisionId = Target_ ? Target_->RevisionId : target->RevisionId;
        wrappedError = TError("Failed to prepare file snapshot")
            .With("snapshot_role", role == ESnapshotRole::Active ? "active" : "preparing")
            .With("snapshot_state", slot.State)
            .With("snapshot_id", fileSnapshot->Id)
            .With("revision_id", revisionId)
            .With(error);
        if (slot.PreparationStage) {
            wrappedError.Add(TErrorAttribute("preparation_stage", *slot.PreparationStage));
        }

        THashMap<TFileSourceId, std::string> objectIds;
        THashMap<TFileSourceId, std::string> displayVersions;
        for (const auto& [name, revision] : fileSnapshot->FileSources) {
            if (revision) {
                objectIds[name] = revision->ObjectId.Underlying();
                displayVersions[name] = revision->DisplayVersion;
            }
        }
        if (!objectIds.empty()) {
            wrappedError
                .Add(TErrorAttribute("file_source_object_ids", objectIds))
                .Add(TErrorAttribute("file_source_display_versions", displayVersions));
        }

        retryPeriod = GetUpdateRetryPeriod();
        slot.InFlight = false;
        slot.State = EFileSnapshotState::Preparing;
        slot.PreparationStage = EFileSnapshotPreparationStage::Waiting;
        slot.Error = wrappedError;
        slot.NextRetryAt = TInstant::Now() + retryPeriod;
        discardedCandidate = std::move(slot.Candidate);
        SchedulePreparation(guard);
    }

    YT_TLOG_WARNING("Failed to prepare file snapshot")
        .With(wrappedError);
    if (UpdateError_) {
        UpdateError_->SetError(wrappedError);
    }

    NConcurrency::TDelayedExecutor::Submit(
        BIND([
            weakThis = MakeWeak(this),
            role,
            target,
            fileSnapshot,
            attemptGeneration
        ] {
            if (auto strongThis = weakThis.Lock()) {
                auto guard = Guard(strongThis->Lock_);
                if (!strongThis->IsCurrentAttempt(
                    role,
                    target,
                    fileSnapshot,
                    attemptGeneration))
                {
                    return;
                }

                auto& slot = strongThis->GetSlot(role);
                if (slot.InFlight || !slot.NextRetryAt) {
                    return;
                }
                slot.NextRetryAt.reset();
                strongThis->SchedulePreparation(guard);
            }
        }),
        retryPeriod,
        GetContext()->Invoker);
}

template <class TData>
bool TFileResourceBase<TData>::SetSnapshotPreparationStage(
    ESnapshotRole role,
    const TResourceRevisionPtr& target,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration,
    EFileSnapshotPreparationStage stage)
{
    auto guard = Guard(Lock_);
    if (!IsCurrentAttempt(role, target, fileSnapshot, attemptGeneration) ||
        !GetSlot(role).InFlight)
    {
        return false;
    }
    auto& slot = GetSlot(role);
    slot.State = EFileSnapshotState::Preparing;
    slot.PreparationStage = stage;
    return true;
}

template <class TData>
bool TFileResourceBase<TData>::IsCurrentAttempt(
    ESnapshotRole role,
    const TResourceRevisionPtr& /*target*/,
    const TFileSnapshotPtr& fileSnapshot,
    ui64 attemptGeneration) const
{
    const auto& slot = GetSlot(role);
    return slot.Desired &&
        fileSnapshot &&
        slot.Desired->Id == fileSnapshot->Id &&
        slot.AttemptGeneration == attemptGeneration;
}

template <class TData>
typename TFileResourceBase<TData>::TSnapshotSlot& TFileResourceBase<TData>::GetSlot(ESnapshotRole role)
{
    return role == ESnapshotRole::Active ? ActiveSlot_ : PreparingSlot_;
}

template <class TData>
const typename TFileResourceBase<TData>::TSnapshotSlot& TFileResourceBase<TData>::GetSlot(ESnapshotRole role) const
{
    return role == ESnapshotRole::Active ? ActiveSlot_ : PreparingSlot_;
}

template <class TData>
TFileSnapshotStatusPtr TFileResourceBase<TData>::BuildSnapshotStatus(const TSnapshotSlot& slot) const
{
    if (!slot.Desired) {
        return nullptr;
    }

    auto status = New<TFileSnapshotStatus>();
    status->SnapshotId = slot.Desired->Id;
    status->State = slot.State;
    status->PreparationStage = slot.PreparationStage;
    status->Error = slot.Error;
    status->NextRetryAt = slot.NextRetryAt;
    return status;
}

template <class TData>
TDuration TFileResourceBase<TData>::GetUpdateRetryPeriod() const
{
    return UpdateRetryPeriod_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
