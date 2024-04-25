#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourcesConsumerType,
    ((MasterJob)             (0))
    ((SchedulerAllocation)   (1))
);

class TJobResourceManager
    : public TRefCounted
{
protected:
    class TResourceAcquiringContext;
    class TImpl;

public:
    virtual void Initialize() = 0;

    virtual void Start() = 0;

    //! Returns the maximum allowed resource usage.
    virtual NClusterNode::TJobResources GetResourceLimits() const = 0;

    virtual NNodeTrackerClient::NProto::TDiskResources GetDiskResources() const = 0;

    //! Set resource limits overrides.
    virtual void SetResourceLimitsOverrides(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimits) = 0;

    virtual double GetCpuToVCpuFactor() const = 0;

    //! Returns resource usage of running jobs.
    virtual NClusterNode::TJobResources GetResourceUsage(bool includePending = false) const = 0;

    //! Compares new usage with resource limits. Detects resource overdraft.
    virtual bool CheckMemoryOverdraft(const NClusterNode::TJobResources& delta) = 0;

    virtual TResourceAcquiringContext GetResourceAcquiringContext() = 0;

    virtual int GetPendingResourceHolderCount() = 0;

    virtual void RegisterResourcesConsumer(TClosure onResourcesReleased, EResourcesConsumerType consumer) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void OnDynamicConfigChanged(
        const TJobResourceManagerDynamicConfigPtr& oldConfig,
        const TJobResourceManagerDynamicConfigPtr& newConfig) = 0;

    static TJobResourceManagerPtr CreateJobResourceManager(NClusterNode::IBootstrapBase* bootstrap);

    DECLARE_INTERFACE_SIGNAL(void(), ResourcesAcquired);
    DECLARE_INTERFACE_SIGNAL(void(EResourcesConsumerType, bool), ResourcesReleased);
    DECLARE_INTERFACE_SIGNAL(void(TResourceHolderPtr), ResourceUsageOverdraftOccurred);

    DECLARE_INTERFACE_SIGNAL(
        void(i64 mapped),
        ReservedMemoryOvercommited);

protected:
    friend TResourceHolder;

    class TResourceAcquiringContext
    {
    public:
        explicit TResourceAcquiringContext(TJobResourceManager* resourceManagerImpl);
        TResourceAcquiringContext(const TResourceAcquiringContext&) = delete;
        ~TResourceAcquiringContext();

        bool TryAcquireResourcesFor(const TResourceHolderPtr& resourceHolder) &;

    private:
        NConcurrency::TForbidContextSwitchGuard Guard_;
        TImpl* const ResourceManagerImpl_;
    };
};

DEFINE_REFCOUNTED_TYPE(TJobResourceManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourcesState,
    ((Pending)   (0))
    ((Acquired)  (1))
    ((Released)  (2))
);

////////////////////////////////////////////////////////////////////////////////

class TResourceOwner
    : public TRefCounted
{
public:
    TResourceOwner(
        TGuid holderId,
        TJobResourceManager* jobResourceManager,
        EResourcesConsumerType resourceConsumerType,
        const NClusterNode::TJobResources& jobResources);

    void ReleaseResources();

protected:
    TResourceHolderPtr ResourceHolder_;
};

DEFINE_REFCOUNTED_TYPE(TResourceOwner)

class TResourceHolder
    : public TRefCounted
{
public:
    const EResourcesConsumerType ResourcesConsumerType;

    static TResourceHolderPtr CreateResourceHolder(
        TGuid id,
        TJobResourceManager* jobResourceManager,
        EResourcesConsumerType resourceConsumerType,
        const NClusterNode::TJobResources& jobResources);

    TResourceHolder(
        TGuid id,
        TJobResourceManager* jobResourceManager,
        EResourcesConsumerType resourceConsumerType,
        const NClusterNode::TJobResources& jobResources);

    TResourceHolder(const TResourceHolder&) = delete;
    TResourceHolder(TResourceHolder&&) = delete;
    ~TResourceHolder();

    TGuid GetId() const noexcept;

    const std::vector<int>& GetPorts() const noexcept;
    const NClusterNode::ISlotPtr& GetUserSlot() const noexcept;
    const std::vector<NClusterNode::ISlotPtr>& GetGpuSlots() const noexcept;

    //! Returns true unless overcommit occurred.
    bool SetBaseResourceUsage(NClusterNode::TJobResources newResourceUsage);
    bool UpdateAdditionalResourceUsage(NClusterNode::TJobResources additionalResourceUsageDelta);

    IMemoryUsageTrackerPtr GetAdditionalMemoryUsageTracker(EMemoryCategory memoryCategory);

    void ReleaseNonSlotResources();
    void ReleaseBaseResources();

    NClusterNode::TJobResources GetResourceUsage() const noexcept;

    std::pair<NClusterNode::TJobResources, NClusterNode::TJobResources> GetDetailedResourceUsage() const noexcept;

    const NClusterNode::TJobResourceAttributes& GetResourceAttributes() const noexcept;

    NClusterNode::TJobResources GetResourceLimits() const noexcept;

    NClusterNode::TJobResources GetFreeResources() const noexcept;

    void UpdateResourceDemand(
        const NClusterNode::TJobResources& jobResources,
        const NClusterNode::TJobResourceAttributes& resourceAttributes,
        int portCount);

    const NLogging::TLogger& GetLogger() const noexcept;

    TResourceOwnerPtr GetOwner() const noexcept;
    void ResetOwner(const TResourceOwnerPtr& owner);

private:
    const TGuid Id_;

    const NLogging::TLogger Logger;

    TWeakPtr<TResourceOwner> Owner_;

    NClusterNode::ISlotPtr UserSlot_;

    std::vector<NClusterNode::ISlotPtr> GpuSlots_;

    friend TJobResourceManager::TResourceAcquiringContext;
    friend TJobResourceManager::TImpl;

    TJobResourceManager::TImpl* const ResourceManagerImpl_;

    int PortCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ResourcesLock_);

    NClusterNode::TJobResources BaseResourceUsage_;
    NClusterNode::TJobResources AdditionalResourceUsage_;
    NClusterNode::TJobResourceAttributes ResourceAttributes_;

    std::vector<int> Ports_;

    EResourcesState State_ = EResourcesState::Pending;

    void Register();
    void Unregister();

    class TAcquiredResources;
    void SetAcquiredResources(TAcquiredResources&& acquiredResources);

    void ReleaseAdditionalResources();

    NClusterNode::TJobResources CumulativeResourceUsage() const noexcept;

    struct TResourceHolderInfo
    {
        TGuid Id;
        NClusterNode::TJobResources BaseResourceUsage;
        NClusterNode::TJobResources AdditionalResourceUsage;
        EResourcesConsumerType ResourcesConsumerType;
    };

    TResourceHolderInfo BuildResourceHolderInfo() const noexcept;

    template <class TResourceUsageUpdater>
        requires std::is_invocable_r_v<
            NClusterNode::TJobResources,
            TResourceUsageUpdater,
            const NClusterNode::TJobResources&>
    //! Semantic requirement: TResourceUsageUpdater::operator() must return delta between
    //! cumulative resource usages before and after the call to this function.
    bool DoSetResourceUsage(
        const NClusterNode::TJobResources& resourceUsage,
        TStringBuf argumentName,
        TResourceUsageUpdater resourceUsageUpdater);
};

DEFINE_REFCOUNTED_TYPE(TResourceHolder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
