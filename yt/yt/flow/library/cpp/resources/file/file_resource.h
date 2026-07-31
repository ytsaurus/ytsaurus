#pragma once

#include "file_source_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/resource_controller_base.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TFileResourceParameters
    : public virtual NYTree::TYsonStruct
{
    TFileSourceSpecPtr FileSource;

    REGISTER_YSON_STRUCT(TFileResourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileResourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TFileResourceDynamicParameters
    : public virtual NYTree::TYsonStruct
{
    TDuration DiscoverPeriod;
    TDuration UpdateRetryPeriod;

    REGISTER_YSON_STRUCT(TFileResourceDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileResourceDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

struct TFileResourceValidator
{
    static void Validate(const TResourceSpec& spec);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMaterializedDirectory);

class TMaterializedDirectory
    : public TRefCounted
{
public:
    TMaterializedDirectory(
        TFileSourceRevisionPtr revision,
        NFileStorage::IFileStorageObjectPtr storageObject);

    const TFileSourceRevisionPtr& GetRevision() const;
    const std::string& GetRootPath() const;

private:
    const TFileSourceRevisionPtr Revision_;
    const NFileStorage::IFileStorageObjectPtr StorageObject_;
    const std::string RootPath_;
};

DEFINE_REFCOUNTED_TYPE(TMaterializedDirectory);

////////////////////////////////////////////////////////////////////////////////

struct TFileResourceControllerState
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr FileSource;
    TFileSourceRevisionPtr Revision;

    REGISTER_YSON_STRUCT(TFileResourceControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileResourceControllerState);

////////////////////////////////////////////////////////////////////////////////

class TFileResourceController
    : public TResourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TFileResourceParameters, TResourceControllerBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TFileResourceDynamicParameters, TResourceControllerBase);

    TFileResourceController(
        TResourceControllerContextPtr context,
        TDynamicResourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    NYTree::INodePtr BuildTargetRevisionSpec() override;
    void CollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) override;
    NYTree::IMapNodePtr GetView() override;

private:
    void Discover();

    const IFileSourcePtr Source_;
    const IStatusErrorStatePtr DiscoveryError_;
    const NConcurrency::TPeriodicExecutorPtr DiscoveryExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TMutableStateClient<TFileResourceControllerState> State_;
    TFileSourceRevisionPtr Revision_;
    THashMap<std::pair<i64, std::string>, i64> RevisionCounts_;
    THashMap<EFileResourceUpdateState, i64> UpdateStateCounts_;
    THashMap<std::pair<i64, std::string>, NProfiling::TGauge> RevisionGauges_;
};

DEFINE_REFCOUNTED_TYPE(TFileResourceController);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TData>
struct TFileResourceSnapshot
    : public TRefCounted
{
    TFileResourceSnapshot(
        TIntrusivePtr<TData> data,
        TMaterializedDirectoryPtr directory,
        TFileSourceRevisionPtr sourceRevision,
        i64 deliveryRevisionId)
        : Directory(std::move(directory))
        , Data(std::move(data))
        , SourceRevision(std::move(sourceRevision))
        , DeliveryRevisionId(deliveryRevisionId)
    { }

    // #Data is destroyed before #Directory, so its destructor may still inspect cached input.
    const TMaterializedDirectoryPtr Directory;
    const TIntrusivePtr<TData> Data;
    const TFileSourceRevisionPtr SourceRevision;
    const i64 DeliveryRevisionId;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TData>
class TFileResourceAccessor
{
public:
    const TData& operator*() const;
    const TData* operator->() const;

    const TFileSourceRevisionPtr& GetSourceRevision() const;
    i64 GetDeliveryRevisionId() const;

private:
    using TSnapshot = NDetail::TFileResourceSnapshot<TData>;

    explicit TFileResourceAccessor(TIntrusivePtr<TSnapshot> snapshot);

    const TIntrusivePtr<TSnapshot> Snapshot_;

    template <class>
    friend class TFileResourceBase;
};

////////////////////////////////////////////////////////////////////////////////

template <class TData>
class TFileResourceBase
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TFileResourceParameters, TResourceBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TFileResourceDynamicParameters, TResourceBase);
    YT_FLOW_EXTEND_SPEC_VALIDATION(TFileResourceValidator::Validate);

    using TController = TFileResourceController;
    using TDataPtr = TIntrusivePtr<TData>;
    using TAccessor = TFileResourceAccessor<TData>;

    TFileResourceBase(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext);

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) final;
    TResourceRevisionState GetRevisionState() const final;
    TAccessor Lock() const;

protected:
    virtual TDataPtr Initialize(const TMaterializedDirectoryPtr& directory) = 0;
    virtual void Validate(const TDataPtr& data);

private:
    using TSnapshot = NDetail::TFileResourceSnapshot<TData>;
    using TSnapshotPtr = TIntrusivePtr<TSnapshot>;

    void OnReconfigured(const TDynamicResourceContextPtr& dynamicContext);
    void ScheduleProcessing(const TGuard<NThreading::TSpinLock>& guard);
    void ProcessTargets();
    TSnapshotPtr BuildCandidate(
        const TResourceRevisionPtr& target,
        const TFileSourceRevisionPtr& sourceRevision);
    TFileSourceRevisionPtr ParseTarget(const TResourceRevisionPtr& target) const;
    void SetUpdateState(EFileResourceUpdateState state);

    const IFileSourcePtr Source_;
    const NFileStorage::IFileStoragePtr FileStorage_;
    const IStatusErrorStatePtr UpdateError_;
    TAtomicIntrusivePtr<TSnapshot> ActiveSnapshot_;

    mutable NThreading::TSpinLock Lock_;
    TResourceRevisionPtr Target_;
    std::optional<i64> AppliedRevisionId_;
    std::optional<EFileResourceUpdateState> UpdateState_;
    bool LoadStarted_ = false;
    bool Processing_ = false;
    bool RetryScheduled_ = false;
    bool InitialLoadCompleted_ = false;
    TPromise<void> InitialLoadPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define FILE_RESOURCE_INL_H_
#include "file_resource-inl.h"
#undef FILE_RESOURCE_INL_H_
