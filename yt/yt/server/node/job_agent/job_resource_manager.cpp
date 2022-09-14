#include "job_resource_manager.h"

#include "job_controller.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/net/helpers.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NProfiling;
using namespace NNodeTrackerClient;
using namespace NNet;
using namespace NLogging;

using NNodeTrackerClient::NProto::TNodeResources;
using NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides;
using NNodeTrackerClient::NProto::TDiskResources;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TResourceHolder::TAcquiredResources
{
public:
    TAcquiredResources(
        IJobResourceManager::TImpl* jobResourceManagerImpl,
        TMemoryUsageTrackerGuard&& userMemoryGuard,
        TMemoryUsageTrackerGuard&& systemMemoryGuard,
        std::vector<int> ports) noexcept;
    ~TAcquiredResources();

    TMemoryUsageTrackerGuard UserMemoryGuard;
    TMemoryUsageTrackerGuard SystemMemoryGuard;
    std::vector<int> Ports;

private:
    IJobResourceManager::TImpl* const JobResourceManagerImpl_;
};

////////////////////////////////////////////////////////////////////////////////

class IJobResourceManager::TImpl
    : public IJobResourceManager
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), ResourcesUpdated);

    DEFINE_SIGNAL_OVERRIDE(
        void(i64 mapped),
        ReservedMemoryOvercommited);
    
    DEFINE_SIGNAL_OVERRIDE(void(), ResourcesReleased);

public:
    explicit TImpl(IBootstrapBase* bootstrap)
        : Config_(bootstrap->GetConfig()->ExecNode->JobController)
        , Bootstrap_(bootstrap)
        , NodeMemoryUsageTracker_(Bootstrap_->GetMemoryUsageTracker())
        , SystemMemoryUsageTracker_(NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::SystemJobs))
        , UserMemoryUsageTracker_(NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::UserJobs))
        , Profiler_("/job_controller")
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Bootstrap_);
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        Profiler_.AddProducer("/resource_limits", ResourceLimitsBuffer_);
        Profiler_.AddProducer("/resource_usage", ResourceUsageBuffer_);

        if (Config_->PortSet) {
            FreePorts_ = *Config_->PortSet;
        } else {
            for (int index = 0; index < Config_->PortCount; ++index) {
                FreePorts_.insert(Config_->StartPort + index);
            }
        }
    }

    void Initialize() override
    {
        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TImpl::OnProfiling, MakeWeak(this)),
            Config_->ProfilingPeriod);
        ProfilingExecutor_->Start();

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
            dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
        
        if (Config_->MappedMemoryController) {
            ReservedMappedMemoryChecker_ = New<TPeriodicExecutor>(
                Bootstrap_->GetJobInvoker(),
                BIND_NO_PROPAGATE(&TImpl::CheckReservedMappedMemory, MakeWeak(this)),
                Config_->MappedMemoryController->CheckPeriod);
            ReservedMappedMemoryChecker_->Start();
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto jobControllerConfig = newNodeConfig->ExecNode->JobController;
        YT_ASSERT(jobControllerConfig);
        DynamicConfig_.Store(jobControllerConfig);

        ProfilingExecutor_->SetPeriod(
            jobControllerConfig->ProfilingPeriod.value_or(
                Config_->ProfilingPeriod));
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ResourceUsageBuffer_->Update([this] (ISensorWriter* writer) {
            ProfileResources(writer, GetResourceUsage());
        });

        ResourceLimitsBuffer_->Update([this] (ISensorWriter* writer) {
            ProfileResources(writer, GetResourceLimits());
        });
    }

    TNodeResources GetResourceLimits() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TNodeResources result;

        // If chunk cache is disabled, we disable all scheduler jobs.
        bool chunkCacheEnabled = false;
        if (Bootstrap_->IsExecNode()) {
            const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
            chunkCacheEnabled = chunkCache->IsEnabled();

            auto slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
            result.set_user_slots(
                chunkCacheEnabled && !Bootstrap_->GetJobController()->AreSchedulerJobsDisabled() && !Bootstrap_->IsReadOnly() && slotManager->IsEnabled()
                ? slotManager->GetSlotCount()
                : 0);
        } else {
            result.set_user_slots(0);
        }

        auto resourceLimitsOverrides = ResourceLimitsOverrides_.Load();
        #define XX(name, Name) \
            result.set_##name(resourceLimitsOverrides.has_##name() \
                ? resourceLimitsOverrides.name() \
                : Config_->ResourceLimits->Name);
        ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
        #undef XX

        if (Bootstrap_->IsExecNode()) {
            const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
            result.set_gpu(gpuManager->GetTotalGpuCount());
        } else {
            result.set_gpu(0);
        }

        // NB: Some categories can have no explicit limit.
        // Therefore we need bound memory limit by actually available memory.
        auto getUsedMemory = [&] (ITypedNodeMemoryTracker* memoryUsageTracker) {
            return std::max<i64>(
                0,
                memoryUsageTracker->GetUsed() + NodeMemoryUsageTracker_->GetTotalFree() - Config_->FreeMemoryWatermark);
        };
        result.set_user_memory(std::min(
            UserMemoryUsageTracker_->GetLimit(),
            getUsedMemory(UserMemoryUsageTracker_.Get())));
        result.set_system_memory(std::min(
            SystemMemoryUsageTracker_->GetLimit(),
            getUsedMemory(SystemMemoryUsageTracker_.Get())));

        const auto& nodeResourceManager = Bootstrap_->GetNodeResourceManager();
        result.set_cpu(nodeResourceManager->GetJobsCpuLimit());
        result.set_vcpu(static_cast<double>(NVectorHdrf::TCpuResource(result.cpu() * GetCpuToVCpuFactor())));

        return result;
    }

    TNodeResources GetResourceUsage(bool includeWaiting = false) const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto result = ResourceUsage_;
        if (includeWaiting) {
            result += WaitingResources_;
        }
        
        if (Bootstrap_->IsExecNode()) {
            const auto& slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
            auto userSlotCount = slotManager->GetUsedSlotCount();

            // TSlotManager::ReleaseSlot is async, so TSlotManager::GetUsedSlotCount() may be greater than ResourceUsage_.user_slots().
            YT_LOG_FATAL_IF(
                userSlotCount < ResourceUsage_.user_slots(),
                "Unexpected user slot count (JobResourcesManagerValue: %v, SlotManagerValue: %v, SlotManagerEnabled: %v)",
                ResourceUsage_.user_slots(),
                userSlotCount,
                slotManager->IsEnabled());
            
            result.set_user_slots(slotManager->IsEnabled() ? userSlotCount : 0);

            const auto& gpuManager = Bootstrap_->GetExecNodeBootstrap()->GetGpuManager();
            result.set_gpu(gpuManager->GetUsedGpuCount());
        } else {
            result.set_user_slots(0);
            result.set_gpu(0);
        }

        result.set_vcpu(static_cast<double>(NVectorHdrf::TCpuResource(result.cpu() * GetCpuToVCpuFactor())));

        return result;
    }

    bool CheckMemoryOverdraft(const TNodeResources& delta) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        // Only "cpu" and "user_memory" can be increased.
        // Network decreases by design. Cpu increasing is handled in AdjustResources.
        // Other resources are not reported by job proxy (see TSupervisorService::UpdateResourceUsage).

        if (delta.user_memory() > 0) {
            bool watermarkReached = NodeMemoryUsageTracker_->GetTotalFree() <= Config_->FreeMemoryWatermark;
            if (watermarkReached) {
                return true;
            }

            auto error = UserMemoryUsageTracker_->TryAcquire(delta.user_memory());
            if (!error.IsOK()) {
                return true;
            }
        }

        return false;
    }

    void OnResourceAcquiringStarted()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(!std::exchange(HasActiveResourceAcquiring_, true));

        YT_VERIFY(!std::exchange(ShouldNotifyResourcesUpdated_, false));

        auto usedResources = GetResourceUsage();

        auto releaseMemoryIfNeeded = [&] (i64 memoryToRelease, ITypedNodeMemoryTracker* memoryUsageTracker) {
            if (memoryToRelease > 0) {
                memoryUsageTracker->Release(memoryToRelease);
                ShouldNotifyResourcesUpdated_ = true;
            }
        };

        auto memoryToRelease = UserMemoryUsageTracker_->GetUsed() - usedResources.user_memory();
        releaseMemoryIfNeeded(memoryToRelease, UserMemoryUsageTracker_.Get());
        memoryToRelease = SystemMemoryUsageTracker_->GetUsed() - usedResources.system_memory();
        releaseMemoryIfNeeded(memoryToRelease, SystemMemoryUsageTracker_.Get());
    }

    void OnResourceAcquiringFinished()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(std::exchange(HasActiveResourceAcquiring_, false));

        if (ShouldNotifyResourcesUpdated_) {
            ResourcesUpdated_.Fire();
            ShouldNotifyResourcesUpdated_ = false;
        }
    }

    void OnResourceHolderCreated(const TLogger& Logger, const TNodeResources& resources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        WaitingResources_ += resources;
        YT_LOG_DEBUG("Resource holder created, resources waiting (Resources: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resources),
            FormatResources(ResourceUsage_),
            FormatResources(WaitingResources_));
    }

    void OnResourcesAcquired(const TLogger& Logger, const TNodeResources& resources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        WaitingResources_ -= resources;
        ResourceUsage_ += resources;

        YT_LOG_DEBUG("Resources acquired (Resources: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resources),
            FormatResources(ResourceUsage_),
            FormatResources(WaitingResources_));

        ShouldNotifyResourcesUpdated_ = true;
    }

    void OnResourcesReleased(
        const TLogger& Logger,
        const TNodeResources& resources,
        const std::vector<int>& ports,
        bool resourceHolderStarted)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(resourceHolderStarted || ports.empty());

        ReleasePorts(Logger, ports);

        if (resourceHolderStarted) {
            ResourceUsage_ -= resources;
        } else {
            WaitingResources_ -= resources;
        }

        YT_LOG_DEBUG("Resources released (ResourceHolderStarted: %v, Delta: %v, ResourceUsage: %v, WaitingResources: %v)",
            resourceHolderStarted,
            FormatResources(resources),
            FormatResources(ResourceUsage_),
            FormatResources(WaitingResources_));
        
        ResourcesReleased_.Fire();
    }

    void OnResourcesUpdated(const TLogger& Logger, const TNodeResources& resourceDelta)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ResourceUsage_ += resourceDelta;

        YT_LOG_DEBUG("Resource usage updated (Delta: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resourceDelta),
            FormatResources(ResourceUsage_),
            FormatResources(WaitingResources_));
        
        if (!Dominates(resourceDelta, ZeroNodeResources())) {
            ResourcesReleased_.Fire();
        }
    }

    TDiskResources GetDiskResources() const override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (Bootstrap_->IsExecNode()) {
            const auto& slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
            return slotManager->GetDiskResources();
        } else {
            return TDiskResources{};
        }
    }

    void SetResourceLimitsOverrides(const TNodeResourceLimitsOverrides& resourceLimits) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ResourceLimitsOverrides_.Store(resourceLimits);
    }

    double GetCpuToVCpuFactor() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto dynamicConfig = DynamicConfig_.Load();
        if (dynamicConfig && dynamicConfig->EnableCpuToVCpuFactor) {
            if (dynamicConfig->CpuToVCpuFactor) {
                return dynamicConfig->CpuToVCpuFactor.value();
            }
            if (Config_->CpuToVCpuFactor) {
                return Config_->CpuToVCpuFactor.value();
            }
            if (dynamicConfig->CpuModelToCpuToVCpuFactor && Config_->CpuModel) {
                const auto& cpuModel = *Config_->CpuModel;
                const auto& cpuModelToCpuToVCpuFactor = *dynamicConfig->CpuModelToCpuToVCpuFactor;
                if (auto it = cpuModelToCpuToVCpuFactor.find(cpuModel); it != cpuModelToCpuToVCpuFactor.end()) {
                    return it->second;
                }
            }
        }

        return 1.0;
    }

    bool AcquireResourcesFor(
        TResourceHolder* resourceHolder)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto& neededResources = resourceHolder->Resources_;
        auto portCount = resourceHolder->PortCount_;

        const auto& Logger = resourceHolder->GetLogger();

        YT_LOG_DEBUG(
            "Trying to acquire resources (NeededResources: %v, PortCount: %v)",
            FormatResources(neededResources),
            portCount);

        auto resourceUsage = GetResourceUsage();
        if (!HasEnoughResources(neededResources, resourceUsage)) {
            YT_LOG_DEBUG("Not enough resources (NeededResources: %v, ResourceUsage: %v)",
                FormatResources(neededResources),
                FormatResourceUsage(resourceUsage, GetResourceLimits()));
            return false;
        }

        i64 userMemory = neededResources.user_memory();
        i64 systemMemory = neededResources.system_memory();
        if (userMemory > 0 || systemMemory > 0) {
            bool reachedWatermark = NodeMemoryUsageTracker_->GetTotalFree() <= Config_->FreeMemoryWatermark;
            if (reachedWatermark) {
                YT_LOG_DEBUG("Not enough memory; reached free memory watermark");
                return false;
            }
        }

        TMemoryUsageTrackerGuard userMemoryGuard;
        TMemoryUsageTrackerGuard systemMemoryGuard;

        if (userMemory > 0) {
            auto errorOrGuard = TMemoryUsageTrackerGuard::TryAcquire(UserMemoryUsageTracker_, userMemory);
            if (!errorOrGuard.IsOK()) {
                YT_LOG_DEBUG(errorOrGuard, "Not enough user memory");
                return false;
            }

            userMemoryGuard = std::move(errorOrGuard.Value());
        }

        if (systemMemory > 0) {
            auto errorOrGuard = TMemoryUsageTrackerGuard::TryAcquire(SystemMemoryUsageTracker_, systemMemory);
            if (!errorOrGuard.IsOK()) {
                YT_LOG_DEBUG(errorOrGuard, "Not enough system memory");
                return false;
            }

            systemMemoryGuard = std::move(errorOrGuard.Value());
        }

        std::vector<int> ports;

        if (portCount > 0) {
            YT_LOG_INFO("Allocating ports (PortCount: %v)", portCount);

            try {
                ports = AllocateFreePorts(portCount, FreePorts_, Logger);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Error while allocating free ports (PortCount: %v)", portCount);
                return false;
            }

            if (std::ssize(ports) < portCount) {
                YT_LOG_DEBUG("Not enough bindable free ports (PortCount: %v, FreePortCount: %v)",
                    portCount,
                    ports.size());
                return false;
            }

            for (int port : ports) {
                FreePorts_.erase(port);
            }

            YT_LOG_DEBUG("Ports allocated (PortCount: %v, Ports: %v)", ports.size(), ports);
        }

        YT_LOG_DEBUG("Resources successfully allocated");

        resourceHolder->AcquireResources({
            this,
            std::move(userMemoryGuard),
            std::move(systemMemoryGuard),
            std::move(ports)});

        return true;
    }

    void ReleasePorts(const TLogger& Logger, const std::vector<int>& ports)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Releasing ports (PortCount: %v, Ports: %v)",
            ports.size(),
            ports);
        for (auto port : ports) {
            InsertOrCrash(FreePorts_, port);
        }
    }

    TResourceAcquiringProxy GetResourceAcquiringProxy() override
    {
        return TResourceAcquiringProxy{this};
    }

private:
    const TIntrusivePtr<const TJobControllerConfig> Config_;
    IBootstrapBase* const Bootstrap_;

    TAtomicObject<TJobControllerDynamicConfigPtr> DynamicConfig_ = New<TJobControllerDynamicConfig>();

    TAtomicObject<TNodeResourceLimitsOverrides> ResourceLimitsOverrides_;

    const INodeMemoryTrackerPtr NodeMemoryUsageTracker_;
    const ITypedNodeMemoryTrackerPtr SystemMemoryUsageTracker_;
    const ITypedNodeMemoryTrackerPtr UserMemoryUsageTracker_;
    THashSet<int> FreePorts_;

    TProfiler Profiler_;
    TPeriodicExecutorPtr ProfilingExecutor_;
    TBufferedProducerPtr ResourceLimitsBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr ResourceUsageBuffer_ = New<TBufferedProducer>();

    TNodeResources ResourceUsage_ = ZeroNodeResources();
    TNodeResources WaitingResources_ = ZeroNodeResources();

    TPeriodicExecutorPtr ReservedMappedMemoryChecker_;

    bool ShouldNotifyResourcesUpdated_ = false;

    bool HasActiveResourceAcquiring_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void CheckReservedMappedMemory()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Check mapped memory usage");

        THashMap<TString, i64> vmstat;
        try {
            vmstat = GetVmstat();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to read /proc/vmstat; skipping mapped memory check");
            return;
        }

        auto mappedIt = vmstat.find("nr_mapped");
        if (mappedIt == vmstat.end()) {
            YT_LOG_WARNING("Field \"nr_mapped\" is not found in /proc/vmstat; skipping mapped memory check");
            return;
        }

        i64 mappedMemory = mappedIt->second;

        YT_LOG_INFO("Mapped memory usage (Usage: %v, Reserved: %v)",
            mappedMemory,
            Config_->MappedMemoryController->ReservedMemory);

        if (mappedMemory <= Config_->MappedMemoryController->ReservedMemory) {
            return;
        }

        ReservedMemoryOvercommited_.Fire(mappedMemory);
    }

    //! Returns |true| if a acquiring with given #neededResources can succeed.
    //! Takes special care with ReplicationDataSize and RepairDataSize enabling
    //! an arbitrary large overdraft for the
    //! first acquiring.
    bool HasEnoughResources(
        const TNodeResources& neededResources,
        const TNodeResources& usedResources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto totalResources = GetResourceLimits();
        auto spareResources = MakeNonnegative(totalResources - usedResources);
        // Allow replication/repair/merge data size overcommit.
        spareResources.set_replication_data_size(InfiniteNodeResources().replication_data_size());
        spareResources.set_repair_data_size(InfiniteNodeResources().repair_data_size());
        spareResources.set_merge_data_size(InfiniteNodeResources().merge_data_size());
        return Dominates(spareResources, neededResources);
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobResourceManagerPtr IJobResourceManager::CreateJobResourceManager(NClusterNode::IBootstrapBase* bootstrap)
{
    return New<IJobResourceManager::TImpl>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

IJobResourceManager::TResourceAcquiringProxy::TResourceAcquiringProxy(IJobResourceManager* resourceManager)
    : ResourceManagerImpl_(static_cast<IJobResourceManager::TImpl*>(resourceManager))
{
    ResourceManagerImpl_->OnResourceAcquiringStarted();
}

IJobResourceManager::TResourceAcquiringProxy::~TResourceAcquiringProxy()
{
    ResourceManagerImpl_->OnResourceAcquiringFinished();
}

bool IJobResourceManager::TResourceAcquiringProxy::TryAcquireResourcesFor(TResourceHolder* resourceHolder) &
{
    if (!ResourceManagerImpl_->AcquireResourcesFor(resourceHolder)) {
        return false;
    }

    try {
        resourceHolder->OnResourcesAcquired();
    } catch (const std::exception& ex) {
        const auto& Logger = resourceHolder->GetLogger();
        YT_LOG_FATAL(ex, "Failed to acquire resources");
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TResourceHolder::TResourceHolder(
    IJobResourceManager* jobResourceManager,
    TLogger logger,
    const TNodeResources& resources,
    int portCount)
    : Logger(std::move(logger))
    , ResourceManagerImpl_(static_cast<IJobResourceManager::TImpl*>(jobResourceManager))
    , PortCount_(portCount)
    , Resources_(resources)
{
    ResourceManagerImpl_->OnResourceHolderCreated(Logger, Resources_);
}

TResourceHolder::~TResourceHolder()
{
    YT_LOG_DEBUG_IF(
        State_ != EResourcesState::Released,
        "Destruct unreleased resoure holder (State: %v, Resources: %v)",
        State_,
        FormatResources(Resources_));
    if (State_ != EResourcesState::Released) {
        ReleaseResources();
    }
}

void TResourceHolder::AcquireResources(TAcquiredResources&& acquiredResources)
{
    YT_VERIFY(State_ == EResourcesState::Waiting);

    Ports_ = std::move(acquiredResources.Ports);

    YT_VERIFY(PortCount_ == std::ssize(Ports_));

    acquiredResources.SystemMemoryGuard.Reset();
    acquiredResources.UserMemoryGuard.Reset();

    ResourceManagerImpl_->OnResourcesAcquired(GetLogger(), Resources_);
    State_ = EResourcesState::Acquired;
}

void TResourceHolder::ReleaseResources()
{
    YT_VERIFY(State_ != EResourcesState::Released);

    ResourceManagerImpl_->OnResourcesReleased(
        Logger,
        Resources_,
        GetPorts(),
        /*resourceHolderStarted*/ State_ == EResourcesState::Acquired);
    State_ = EResourcesState::Released;
    Resources_ = ZeroNodeResources();
}

const std::vector<int>& TResourceHolder::GetPorts() const noexcept
{
    return Ports_;
}

TNodeResources TResourceHolder::SetResourceUsage(TNodeResources newResourceUasge)
{
    YT_VERIFY(State_ == EResourcesState::Acquired);

    auto resourceDelta = newResourceUasge - Resources_;
    ResourceManagerImpl_->OnResourcesUpdated(GetLogger(), resourceDelta);

    Resources_ = newResourceUasge;

    return resourceDelta;
}

const TNodeResources& TResourceHolder::GetResourceUsage() const noexcept
{
    return Resources_;
}

const NLogging::TLogger& TResourceHolder::GetLogger() const noexcept
{
    return Logger;
}

////////////////////////////////////////////////////////////////////////////////

TResourceHolder::TAcquiredResources::TAcquiredResources(
    IJobResourceManager::TImpl* jobResourceManagerImpl,
    TMemoryUsageTrackerGuard&& userMemoryGuard,
    TMemoryUsageTrackerGuard&& systemMemoryGuard,
    std::vector<int> ports) noexcept
    : UserMemoryGuard(std::move(userMemoryGuard))
    , SystemMemoryGuard(std::move(systemMemoryGuard))
    , Ports(std::move(ports))
    , JobResourceManagerImpl_(jobResourceManagerImpl)
{ }

TResourceHolder::TAcquiredResources::~TAcquiredResources()
{
    if (!std::empty(Ports)) {
        JobResourceManagerImpl_->ReleasePorts(NJobAgent::Logger, Ports);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
