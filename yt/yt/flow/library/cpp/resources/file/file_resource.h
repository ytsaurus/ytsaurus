#pragma once

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>
#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TData>
class TFileResourceBase;

////////////////////////////////////////////////////////////////////////////////

struct TFileResourceValidator
{
    static void Validate(const TResourceSpec& spec);
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TFileResourceAccessorState
    : public TRefCounted
{
    std::atomic<i64> LiveAccessorCount = 0;
};

class TFileResourceLifetimeCallback
    : public TRefCounted
{
public:
    explicit TFileResourceLifetimeCallback(TCallback<void()> callback)
        : Callback_(std::move(callback))
    { }

    ~TFileResourceLifetimeCallback() override
    {
        if (Armed_) {
            Callback_();
        }
    }

    void Arm()
    {
        Armed_ = true;
    }

private:
    const TCallback<void()> Callback_;
    bool Armed_ = false;
};

struct TFileResourceLifetimeAnchor
    : public TRefCounted
{
    TIntrusivePtr<TFileResourceLifetimeCallback> RetirementCallback;
};

template <class TData>
struct TFileResourceSnapshot
    : public TRefCounted
{
    TFileResourceSnapshot(
        TIntrusivePtr<TData> data,
        TMaterializedFileSourceSnapshotPtr fileSources,
        TResourceRevisionPtr deliveryRevision,
        TIntrusivePtr<TFileResourceAccessorState> accessorState = nullptr,
        TIntrusivePtr<TFileResourceLifetimeAnchor> lifetimeAnchor = nullptr)
        : LifetimeAnchor(lifetimeAnchor
                ? std::move(lifetimeAnchor)
                : New<TFileResourceLifetimeAnchor>())
        , AccessorState(accessorState
                ? std::move(accessorState)
                : New<TFileResourceAccessorState>())
        , FileSources(std::move(fileSources))
        , Data(std::move(data))
        , DeliveryRevision(std::move(deliveryRevision))
    { }

    const TIntrusivePtr<TFileResourceLifetimeAnchor> LifetimeAnchor;
    const TIntrusivePtr<TFileResourceAccessorState> AccessorState;
    // #Data is destroyed before #FileSources, so its destructor may still inspect cached input.
    const TMaterializedFileSourceSnapshotPtr FileSources;
    const TIntrusivePtr<TData> Data;
    TResourceRevisionPtr DeliveryRevision;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TData>
class TFileResourceAccessor
{
public:
    TFileResourceAccessor(const TFileResourceAccessor& other);
    TFileResourceAccessor(TFileResourceAccessor&& other) noexcept;
    ~TFileResourceAccessor();

    TFileResourceAccessor& operator=(const TFileResourceAccessor& other);
    TFileResourceAccessor& operator=(TFileResourceAccessor&& other) noexcept;

    const TData& operator*() const;
    const TData* operator->() const;

    const TFileSourceRevisionPtr& GetSourceRevision(const TFileSourceId& id) const;
    const std::string& GetRootPath(const TFileSourceId& id) const;
    const TMaterializedFileSourceSnapshotPtr& GetFileSources() const;
    TFileSnapshotId GetFileSnapshotId() const;
    i64 GetDeliveryRevisionId() const;

private:
    using TSnapshot = NDetail::TFileResourceSnapshot<TData>;

    explicit TFileResourceAccessor(TIntrusivePtr<TSnapshot> snapshot);

    void Release();

    TIntrusivePtr<TSnapshot> Snapshot_;

    template <class>
    friend class TFileResourceBase;
};

////////////////////////////////////////////////////////////////////////////////

template <class TData>
class TFileResourceBase
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_SPEC_VALIDATION(TFileResourceValidator::Validate);

    using TDataPtr = TIntrusivePtr<TData>;
    using TAccessor = TFileResourceAccessor<TData>;

    TFileResourceBase(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext);

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) final;
    TResourceRevisionState GetRevisionState() const final;
    TAccessor Lock() const;

protected:
    virtual TDataPtr Initialize(const TMaterializedFileSourceSnapshotPtr& fileSources) = 0;
    virtual void Validate(const TDataPtr& data);

private:
    using TSnapshot = NDetail::TFileResourceSnapshot<TData>;
    using TSnapshotPtr = TIntrusivePtr<TSnapshot>;

    enum class ESnapshotRole
    {
        Active,
        Preparing,
    };

    struct TSnapshotSlot
    {
        TFileSnapshotPtr Desired;
        EFileSnapshotState State = EFileSnapshotState::Preparing;
        std::optional<EFileSnapshotPreparationStage> PreparationStage =
            EFileSnapshotPreparationStage::Waiting;
        ui64 AttemptGeneration = 0;
        TError Error;
        std::optional<TInstant> NextRetryAt;
        TSnapshotPtr Candidate;
        bool InFlight = false;
    };

    struct TActivationState
    {
        std::optional<TFileSnapshotId> PreviousSnapshotId;
        TIntrusivePtr<NDetail::TFileResourceAccessorState> PreviousAccessorState;
        TResourceRevisionPtr DeliveryRevision;
        ui64 AttemptGeneration = 0;
        bool RetiredSnapshotReleased = false;
    };

    void OnReconfigured(const TDynamicResourceContextPtr& dynamicContext);
    void SchedulePreparation(const TGuard<NThreading::TSpinLock>& guard);
    void StartPreparation(
        ESnapshotRole role,
        const TGuard<NThreading::TSpinLock>& guard);
    void BeginPreparation(
        ESnapshotRole role,
        TResourceRevisionPtr target,
        TFileSnapshotPtr fileSnapshot,
        ui64 attemptGeneration);
    void OnMaterialized(
        ESnapshotRole role,
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration,
        const TErrorOr<TMaterializedFileSourceSnapshotPtr>& result);
    void ValidateSnapshot(const TFileSnapshotPtr& fileSnapshot) const;
    void CompletePreparation(
        ESnapshotRole role,
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration,
        TSnapshotPtr candidate);
    void ActivateCandidate(
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration);
    void OnRetiredSnapshotReleased(
        TFileSnapshotId previousSnapshotId,
        ui64 attemptGeneration);
    void ReportActivationStall(
        TFileSnapshotId previousSnapshotId,
        ui64 attemptGeneration,
        TDuration warningPeriod);
    bool TryCompleteActivation(const TGuard<NThreading::TSpinLock>& guard);
    bool CompleteActivation(const TGuard<NThreading::TSpinLock>& guard);
    void ConfigureSlotsAfterActivation(const TGuard<NThreading::TSpinLock>& guard);
    void HandlePreparationError(
        ESnapshotRole role,
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration,
        const TError& error);
    bool SetSnapshotPreparationStage(
        ESnapshotRole role,
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration,
        EFileSnapshotPreparationStage stage);
    bool IsCurrentAttempt(
        ESnapshotRole role,
        const TResourceRevisionPtr& target,
        const TFileSnapshotPtr& fileSnapshot,
        ui64 attemptGeneration) const;
    TSnapshotSlot& GetSlot(ESnapshotRole role);
    const TSnapshotSlot& GetSlot(ESnapshotRole role) const;
    TFileSnapshotStatusPtr BuildSnapshotStatus(const TSnapshotSlot& slot) const;
    TDuration GetUpdateRetryPeriod() const;

    const IStatusErrorStatePtr UpdateError_;
    const IStatusErrorStatePtr ActivationError_;
    const IInvokerPtr InitializationInvoker_;
    const TWeakPtr<TFileResourceBase> WeakThis_;
    TDuration UpdateRetryPeriod_;
    TDuration ActivationStallWarningPeriod_;
    TAtomicIntrusivePtr<TSnapshot> ActiveSnapshot_;

    mutable NThreading::TSpinLock Lock_;
    TResourceRevisionPtr Target_;
    TResourceRevisionPtr AppliedRevision_;
    std::optional<TFileSnapshotId> ActiveFileSnapshotId_;
    TSnapshotSlot ActiveSlot_;
    TSnapshotSlot PreparingSlot_;
    std::optional<TActivationState> ActivationState_;
    ui64 NextAttemptGeneration_ = 0;
    bool LoadStarted_ = false;
    bool InitialLoadCompleted_ = false;
    TPromise<void> InitialLoadPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define FILE_RESOURCE_INL_H_
#include "file_resource-inl.h"
#undef FILE_RESOURCE_INL_H_
