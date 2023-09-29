#include "job_resource_manager.h"

#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/node_resource_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_controller.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/gpu_manager.h>
#include <yt/yt/server/node/exec_node/slot.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/helpers.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NClusterNode;
using namespace NProfiling;
using namespace NNodeTrackerClient;
using namespace NNet;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;

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
        ISlotPtr&& userSlot,
        std::vector<ISlotPtr>&& gpuSlots,
        std::vector<int> ports) noexcept;
    ~TAcquiredResources();

    TMemoryUsageTrackerGuard UserMemoryGuard;
    TMemoryUsageTrackerGuard SystemMemoryGuard;
    ISlotPtr UserSlot;
    std::vector<ISlotPtr> GpuSlots;
    std::vector<int> Ports;

private:
    IJobResourceManager::TImpl* const JobResourceManagerImpl_;
};

////////////////////////////////////////////////////////////////////////////////

class IJobResourceManager::TImpl
    : public IJobResourceManager
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), ResourcesAcquired);
    DEFINE_SIGNAL_OVERRIDE(void(EResourcesConsumerType, bool), ResourcesReleased);

    DEFINE_SIGNAL_OVERRIDE(
        void(i64 mapped),
        ReservedMemoryOvercommited);

public:
    explicit TImpl(IBootstrapBase* bootstrap)
        : Config_(bootstrap->GetConfig()->ExecNode->JobController)
        , Bootstrap_(bootstrap)
        , NodeMemoryUsageTracker_(Bootstrap_->GetMemoryUsageTracker())
        , SystemMemoryUsageTracker_(NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::SystemJobs))
        , UserMemoryUsageTracker_(NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::UserJobs))
        , Profiler_("/job_controller")
        , MajorPageFaultsGauge_(Profiler_.Gauge("/major_page_faults"))
        , FreeMemoryWatermarkMultiplierGauge_(Profiler_.Gauge("/free_memory_watermark_multiplier"))
        , FreeMemoryWatermarkAddedMemoryGauge_(Profiler_.Gauge("/free_memory_watermark_added_memory"))
        , FreeMemoryWatermarkIsIncreasedGauge_(Profiler_.Gauge("/free_memory_watermark_is_increased"))
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

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
            dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        if (Config_->MappedMemoryController) {
            ReservedMappedMemoryChecker_ = New<TPeriodicExecutor>(
                Bootstrap_->GetJobInvoker(),
                BIND_NO_PROPAGATE(&TImpl::CheckReservedMappedMemory, MakeWeak(this)),
                Config_->MappedMemoryController->CheckPeriod);
        }

        if (Bootstrap_->IsExecNode()) {
            MemoryPressureDetector_ = New<TPeriodicExecutor>(
                Bootstrap_->GetJobInvoker(),
                BIND_NO_PROPAGATE(&TImpl::CheckMemoryPressure, MakeWeak(this)),
                DynamicConfig_.Acquire()->MemoryPressureDetector->CheckPeriod);
        }
    }

    void Start() override
    {
        ProfilingExecutor_->Start();
        if (MemoryPressureDetector_) {
            MemoryPressureDetector_->Start();
        }
        if (ReservedMappedMemoryChecker_) {
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

        if (MemoryPressureDetector_) {
            MemoryPressureDetector_->SetPeriod(jobControllerConfig->MemoryPressureDetector->CheckPeriod);
        }
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

        if (Bootstrap_->IsExecNode()) {
            MajorPageFaultsGauge_.Update(LastMajorPageFaultCount_);

            if (FreeMemoryWatermarkMultiplier_ != 1.0 && DynamicConfig_.Acquire()->MemoryPressureDetector->Enabled) {
                FreeMemoryWatermarkMultiplierGauge_.Update(FreeMemoryWatermarkMultiplier_);
                FreeMemoryWatermarkAddedMemoryGauge_.Update(GetFreeMemoryWatermark() - Config_->FreeMemoryWatermark);
                FreeMemoryWatermarkIsIncreasedGauge_.Update(1);
            }
        }
    }

    TJobResources GetResourceLimits() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TJobResources result;
        auto resourceLimitsOverrides = ResourceLimitsOverrides_.Load();

        #define XX(name, Name) \
            result.Name = (resourceLimitsOverrides.has_##name() \
                ? resourceLimitsOverrides.name() \
                : Config_->ResourceLimits->Name);
        ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
        #undef XX

        if (Bootstrap_->IsExecNode()) {
            const auto& execNodeBootstrap = Bootstrap_->GetExecNodeBootstrap();
            auto slotManager = execNodeBootstrap->GetSlotManager();
            auto gpuManager = execNodeBootstrap->GetGpuManager();

            auto scheduleJobEnabled =
                execNodeBootstrap->GetChunkCache()->IsEnabled() &&
                !execNodeBootstrap->GetJobController()->AreSchedulerJobsDisabled() &&
                !Bootstrap_->IsReadOnly() &&
                slotManager->IsEnabled();

            result.UserSlots = scheduleJobEnabled
                ? slotManager->GetSlotCount()
                : 0;

            result.Gpu = resourceLimitsOverrides.has_gpu()
                ? std::min(gpuManager->GetTotalGpuCount(), resourceLimitsOverrides.gpu())
                : gpuManager->GetTotalGpuCount();
        }

        // NB: Some categories can have no explicit limit.
        // Therefore we need bound memory limit by actually available memory.
        auto getUsedMemory = [&] (ITypedNodeMemoryTracker* memoryUsageTracker) {
            return std::max<i64>(
                0,
                memoryUsageTracker->GetUsed() + NodeMemoryUsageTracker_->GetTotalFree() - GetFreeMemoryWatermark());
        };
        result.UserMemory = std::min(
            UserMemoryUsageTracker_->GetLimit(),
            getUsedMemory(UserMemoryUsageTracker_.Get()));
        result.SystemMemory = std::min(
            SystemMemoryUsageTracker_->GetLimit(),
            getUsedMemory(SystemMemoryUsageTracker_.Get()));

        const auto& nodeResourceManager = Bootstrap_->GetNodeResourceManager();
        result.Cpu = nodeResourceManager->GetJobsCpuLimit();
        result.VCpu = static_cast<double>(NVectorHdrf::TCpuResource(result.Cpu * GetCpuToVCpuFactor()));

        return result;
    }

    TJobResources LoadResourceUsage() const
    {
        auto guard = ReaderGuard(ResourcesLock_);
        return ResourceUsage_;
    }

    void SetActualVcpu(TJobResources& resources) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        resources.VCpu = static_cast<double>(NVectorHdrf::TCpuResource(resources.Cpu * GetCpuToVCpuFactor()));
    }

    TJobResources GetResourceUsage(bool includeWaiting = false) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TJobResources resourceUsage;

        std::optional<TJobResources> maybeWaitingResources = std::nullopt;

        {
            auto guard = ReaderGuard(ResourcesLock_);

            resourceUsage = ResourceUsage_;

            if (includeWaiting) {
                maybeWaitingResources = WaitingResources_;
            }
        }

        if (maybeWaitingResources) {
            resourceUsage += WaitingResources_;

            resourceUsage.UserSlots = ResourceUsage_.UserSlots;
            resourceUsage.Gpu = ResourceUsage_.Gpu;
        }

        SetActualVcpu(resourceUsage);

        return resourceUsage;
    }

    bool CheckMemoryOverdraft(const TJobResources& delta) override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        // Only "cpu" and "user_memory" can be increased.
        // Network decreases by design. Cpu increasing is handled in AdjustResources.
        // Other resources are not reported by job proxy (see TSupervisorService::UpdateResourceUsage).

        if (delta.UserMemory > 0) {
            bool watermarkReached = NodeMemoryUsageTracker_->GetTotalFree() <= GetFreeMemoryWatermark();
            if (watermarkReached) {
                return true;
            }

            auto error = UserMemoryUsageTracker_->TryAcquire(delta.UserMemory);
            if (!error.IsOK()) {
                return true;
            }
        }

        return false;
    }

    i64 GetFreeMemoryWatermark() const
    {
        return DynamicConfig_.Acquire()->MemoryPressureDetector->Enabled
            ? Config_->FreeMemoryWatermark * FreeMemoryWatermarkMultiplier_
            : Config_->FreeMemoryWatermark;
    }

    void OnResourceAcquiringStarted()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(!std::exchange(HasActiveResourceAcquiring_, true));

        YT_VERIFY(!std::exchange(ShouldNotifyResourcesUpdated_, false));
    }

    void OnResourceAcquiringFinished()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(std::exchange(HasActiveResourceAcquiring_, false));

        if (ShouldNotifyResourcesUpdated_) {
            ResourcesAcquired_.Fire();
            ShouldNotifyResourcesUpdated_ = false;
        }
    }

    void OnResourceHolderCreated(const TLogger& Logger, const TJobResources& resources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TJobResources currentResourceUsage;
        TJobResources waitingResources;

        {
            auto guard = WriterGuard(ResourcesLock_);
            WaitingResources_ += resources;
            ++WaitingResourceHolderCount_;

            currentResourceUsage = ResourceUsage_;
            waitingResources = WaitingResources_;
        }

        YT_LOG_DEBUG("Resource holder created, resources waiting (Resources: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resources),
            FormatResources(currentResourceUsage),
            FormatResources(waitingResources));
    }

    void OnResourcesReserved(const TLogger& Logger, const TJobResources& resources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TJobResources currentResourceUsage;
        TJobResources waitingResources;

        {
            auto guard = WriterGuard(ResourcesLock_);
            ResourceUsage_ += resources;
            WaitingResources_ -= resources;
            --WaitingResourceHolderCount_;

            currentResourceUsage = ResourceUsage_;
            waitingResources = WaitingResources_;
        }

        YT_LOG_DEBUG("Resources acquired (Resources: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resources),
            FormatResources(currentResourceUsage),
            FormatResources(waitingResources));

        ShouldNotifyResourcesUpdated_ = true;
    }

    void OnResourcesReleased(
        EResourcesConsumerType resourcesConsumerType,
        const TLogger& Logger,
        const TJobResources& resources,
        const std::vector<int>& ports,
        bool resourceHolderStarted)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_VERIFY(resourceHolderStarted || ports.empty());

        ReleasePorts(Logger, ports);

        TJobResources currentResourceUsage;
        TJobResources waitingResources;

        {
            auto guard = WriterGuard(ResourcesLock_);
            if (resourceHolderStarted) {
                ResourceUsage_ -= resources;
            } else {
                WaitingResources_ -= resources;
                --WaitingResourceHolderCount_;
            }

            currentResourceUsage = ResourceUsage_;
            waitingResources = WaitingResources_;
        }

        if (resources.SystemMemory) {
            auto systemMemory = resources.SystemMemory;
            YT_VERIFY(systemMemory >= 0);

            SystemMemoryUsageTracker_->Release(systemMemory);
        }

        if (resources.UserMemory) {
            auto userMemory = resources.UserMemory;
            YT_VERIFY(userMemory >= 0);

            UserMemoryUsageTracker_->Release(userMemory);
        }

        YT_LOG_DEBUG("Resources released (ResourceHolderStarted: %v, Delta: %v, ResourceUsage: %v, WaitingResources: %v)",
            resourceHolderStarted,
            FormatResources(resources),
            FormatResources(currentResourceUsage),
            FormatResources(waitingResources));

        NotifyResourcesReleased(resourcesConsumerType, /*fullyReleased*/ true);
    }

    void OnResourcesUpdated(
        EResourcesConsumerType resourcesConsumerType,
        const TLogger& Logger,
        const TJobResources& resourceDelta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TJobResources currentResourceUsage;
        TJobResources waitingResources;

        {
            auto guard = WriterGuard(ResourcesLock_);
            ResourceUsage_ += resourceDelta;

            currentResourceUsage = ResourceUsage_;
            waitingResources = WaitingResources_;
        }

        auto systemMemory = resourceDelta.SystemMemory;
        if (systemMemory > 0) {
            SystemMemoryUsageTracker_->Acquire(systemMemory);
        } else if (systemMemory < 0) {
            SystemMemoryUsageTracker_->Release(-systemMemory);
        }

        auto userMemory = resourceDelta.UserMemory;
        if (userMemory > 0) {
            UserMemoryUsageTracker_->Acquire(userMemory);
        } else if (userMemory < 0) {
            UserMemoryUsageTracker_->Release(-userMemory);
        }

        YT_LOG_DEBUG("Resource usage updated (Delta: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resourceDelta),
            FormatResources(currentResourceUsage),
            FormatResources(waitingResources));

        if (!Dominates(resourceDelta, ZeroJobResources())) {
            NotifyResourcesReleased(resourcesConsumerType, /*fullyReceived*/ false);
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

        auto dynamicConfig = DynamicConfig_.Acquire();
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

    ISlotPtr AcquireUserSlot(
        const TJobResources& neededResources,
        const TJobResourceAttributes& resourceAttributes)
    {
        YT_VERIFY(Bootstrap_->IsExecNode());

        NScheduler::NProto::TDiskRequest diskRequest;
        diskRequest.set_disk_space(neededResources.DiskSpaceRequest);
        diskRequest.set_inode_count(neededResources.InodeRequest);

        if (resourceAttributes.MediumIndex) {
            diskRequest.set_medium_index(resourceAttributes.MediumIndex.value());
        }

        NScheduler::NProto::TCpuRequest cpuRequest;
        cpuRequest.set_cpu(neededResources.Cpu);
        cpuRequest.set_allow_idle_cpu_policy(resourceAttributes.AllowIdleCpuPolicy);

        YT_LOG_INFO("Acquiring slot (DiskRequest: %v, CpuRequest: %v)",
            diskRequest,
            cpuRequest);

        auto slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
        auto userSlot = slotManager->AcquireSlot(diskRequest, cpuRequest);

        YT_VERIFY(userSlot);

        return userSlot;
    }

    std::vector<ISlotPtr> AcquireGpuSlots(const TJobResources& neededResources)
    {
        YT_VERIFY(Bootstrap_->IsExecNode());

        int gpuCount = neededResources.Gpu;
        YT_LOG_DEBUG("Acquiring GPU slots (Count: %v)", gpuCount);
        auto acquireResult = Bootstrap_
            ->GetExecNodeBootstrap()
            ->GetGpuManager()
            ->AcquireGpuSlots(gpuCount);

        THROW_ERROR_EXCEPTION_IF(
            !acquireResult.IsOK(),
            TError("GPU slot acquisition failed", gpuCount)
                << TErrorAttribute("gpu_count", gpuCount)
                << acquireResult);

        auto result = acquireResult.Value();

        std::vector<ISlotPtr> slots;
        std::vector<int> deviceIndices;

        slots.reserve(result.size());
        deviceIndices.reserve(result.size());

        for (auto& slot : result) {
            deviceIndices.push_back(slot->GetDeviceIndex());
            slots.push_back(std::move(slot));
        }

        YT_LOG_DEBUG("GPU slots acquired (DeviceIndices: %v)", deviceIndices);

        return slots;
    }

    void OnResourcesAcquisitionFailed(
        TResourceHolderPtr resourceHolder,
        ISlotPtr&& userSlot,
        std::vector<ISlotPtr>&& gpuSlots,
        std::vector<int>&& ports,
        TJobResources resources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto& Logger = resourceHolder->GetLogger();

        userSlot.Reset();
        gpuSlots.clear();
        ReleasePorts(Logger, ports);

        TJobResources currentResourceUsage;
        TJobResources waitingResources;

        {
            auto guard = WriterGuard(ResourcesLock_);
            WaitingResources_ += resources;
            ResourceUsage_ -= resources;
            ++WaitingResourceHolderCount_;

            currentResourceUsage = ResourceUsage_;
            waitingResources = WaitingResources_;
        }

        YT_LOG_DEBUG("Resources acquisition failed (Resources: %v, ResourceUsage: %v, WaitingResources: %v)",
            FormatResources(resources),
            FormatResources(currentResourceUsage),
            FormatResources(waitingResources));

        NotifyResourcesReleased(resourceHolder->ResourcesConsumerType_, /*fullyReleased*/ true);
    }

    bool AcquireResourcesFor(TResourceHolderPtr resourceHolder)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        const auto neededResources = resourceHolder->GetResourceUsage();
        const auto& resourceAttributes = resourceHolder->ResourceAttributes_;
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

        if (resourceAttributes.CudaToolkitVersion) {
            YT_VERIFY(Bootstrap_->IsExecNode());

            Bootstrap_
                ->GetExecNodeBootstrap()
                ->GetGpuManager()
                ->VerifyCudaToolkitDriverVersion(resourceAttributes.CudaToolkitVersion.value());
        }

        i64 userMemory = neededResources.UserMemory;
        i64 systemMemory = neededResources.SystemMemory;
        if (userMemory > 0 || systemMemory > 0) {
            bool reachedWatermark = NodeMemoryUsageTracker_->GetTotalFree() <= GetFreeMemoryWatermark();
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

        if (Bootstrap_->IsExecNode()) {
            auto slotManager = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager();
            auto slotManagerCount = slotManager->GetUsedSlotCount();
            auto slotManagerLimit = slotManager->GetSlotCount();
            auto jobResourceManagerCount = LoadResourceUsage().UserSlots;

            YT_LOG_FATAL_IF(
                slotManagerCount != jobResourceManagerCount,
                "Used slot count in slot manager must be equal JobResourceManager count (SlotManagerCount: %v/%v, JobResourceManagerCount: %v)",
                slotManagerCount,
                slotManagerLimit,
                jobResourceManagerCount);
        }

        OnResourcesReserved(resourceHolder->GetLogger(), neededResources);

        ISlotPtr userSlot;
        std::vector<ISlotPtr> gpuSlots;

        try {
            if (neededResources.UserSlots > 0) {
                YT_VERIFY(Bootstrap_->IsExecNode());

                userSlot = AcquireUserSlot(neededResources, resourceAttributes);
            }

            if (neededResources.Gpu > 0) {
                YT_VERIFY(Bootstrap_->IsExecNode());

                gpuSlots = AcquireGpuSlots(neededResources);
            }
        } catch (const std::exception& ex) {
            BIND(&IJobResourceManager::TImpl::OnResourcesAcquisitionFailed,
                MakeStrong(this),
                resourceHolder,
                Passed(std::move(userSlot)),
                Passed(std::move(gpuSlots)),
                Passed(std::move(ports)),
                neededResources)
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run();

            // Provide job abort.
            THROW_ERROR_EXCEPTION(ex);
        }

        resourceHolder->SetAcquiredResources({
            this,
            std::move(userMemoryGuard),
            std::move(systemMemoryGuard),
            std::move(userSlot),
            std::move(gpuSlots),
            std::move(ports)});

        YT_LOG_DEBUG("Resources successfully allocated");

        return true;
    }

    void ReleasePorts(const TLogger& Logger, const std::vector<int>& ports)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO_UNLESS(ports.empty(),
            "Releasing ports (PortCount: %v, Ports: %v)",
            ports.size(),
            ports);
        for (auto port : ports) {
            InsertOrCrash(FreePorts_, port);
        }
    }

    TResourceAcquiringContext GetResourceAcquiringContext() override
    {
        return TResourceAcquiringContext{this};
    }

    int GetWaitingResourceHolderCount() final
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto guard = ReaderGuard(ResourcesLock_);
        return WaitingResourceHolderCount_;
    }

    void RegisterResourcesConsumer(TClosure onResourcesReleased, EResourcesConsumerType consumerType) override
    {
        ResourcesConsumerCallbacks_[consumerType].Subscribe(std::move(onResourcesReleased));
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND_NO_PROPAGATE(
            &IJobResourceManager::TImpl::BuildOrchid,
            MakeStrong(this)));
    }

private:
    const TIntrusivePtr<const TJobControllerConfig> Config_;
    IBootstrapBase* const Bootstrap_;

    TAtomicIntrusivePtr<TJobControllerDynamicConfig> DynamicConfig_{New<TJobControllerDynamicConfig>()};

    TAtomicObject<TNodeResourceLimitsOverrides> ResourceLimitsOverrides_;

    const INodeMemoryTrackerPtr NodeMemoryUsageTracker_;
    const ITypedNodeMemoryTrackerPtr SystemMemoryUsageTracker_;
    const ITypedNodeMemoryTrackerPtr UserMemoryUsageTracker_;
    THashSet<int> FreePorts_;

    TEnumIndexedVector<EResourcesConsumerType, TCallbackList<void()>> ResourcesConsumerCallbacks_;

    TProfiler Profiler_;
    TPeriodicExecutorPtr ProfilingExecutor_;
    TBufferedProducerPtr ResourceLimitsBuffer_ = New<TBufferedProducer>();
    TBufferedProducerPtr ResourceUsageBuffer_ = New<TBufferedProducer>();

    TGauge MajorPageFaultsGauge_;
    TGauge FreeMemoryWatermarkMultiplierGauge_;
    TGauge FreeMemoryWatermarkAddedMemoryGauge_;
    TGauge FreeMemoryWatermarkIsIncreasedGauge_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ResourcesLock_);

    TJobResources ResourceUsage_ = ZeroJobResources();
    TJobResources WaitingResources_ = ZeroJobResources();
    int WaitingResourceHolderCount_ = 0;

    TPeriodicExecutorPtr ReservedMappedMemoryChecker_;
    TPeriodicExecutorPtr MemoryPressureDetector_;

    i64 LastMajorPageFaultCount_ = 0;
    double FreeMemoryWatermarkMultiplier_ = 1.0;

    bool ShouldNotifyResourcesUpdated_ = false;

    bool HasActiveResourceAcquiring_ = false;

    struct TJobResourceManagerInfo
    {
        NClusterNode::TJobResources ResourceLimits;
        NClusterNode::TJobResources ResourceUsage;

        NClusterNode::TJobResources WaitingResources;

        int WaitingResourceHolderCount;

        i64 LastMajorPageFaultCount;

        double FreeMemoryWatermarkMultiplier;

        double CpuToVCpuFactor;

        THashSet<int> FreePorts;
    };

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    TJobResourceManagerInfo DoGetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        TJobResources waitingResources;
        int waitingResourceHolderCount;

        {
            auto guard = ReaderGuard(ResourcesLock_);

            waitingResources = WaitingResources_;
            waitingResourceHolderCount = WaitingResourceHolderCount_;
        }

        return {
            .ResourceLimits = GetResourceLimits(),
            .ResourceUsage = GetResourceUsage(),
            .WaitingResources = waitingResources,
            .WaitingResourceHolderCount = waitingResourceHolderCount,
            .LastMajorPageFaultCount = LastMajorPageFaultCount_,
            .FreeMemoryWatermarkMultiplier = FreeMemoryWatermarkMultiplier_,
            .CpuToVCpuFactor = GetCpuToVCpuFactor(),
            .FreePorts = FreePorts_,
        };
    }

    auto GetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto infoOrError = WaitFor(BIND(
            &IJobResourceManager::TImpl::DoGetStateSnapshot,
            MakeStrong(this))
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run());

        YT_LOG_FATAL_UNLESS(
            infoOrError.IsOK(),
            infoOrError,
            "Unexpected failure while making job resource manager info snapshot");

        return std::move(infoOrError.Value());
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto jobResourceManagerInfo = GetStateSnapshot();

        BuildYsonFluently(consumer).BeginMap()
            .Item("resource_limits").Value(jobResourceManagerInfo.ResourceLimits)
            .Item("resource_usage").Value(ToNodeResources(jobResourceManagerInfo.ResourceUsage))
            .Item("waiting_resources").Value(jobResourceManagerInfo.WaitingResources)
            .Item("waiting_resource_holder_count").Value(jobResourceManagerInfo.WaitingResourceHolderCount)
            .Item("last_major_page_fault_count").Value(jobResourceManagerInfo.LastMajorPageFaultCount)
            .Item("free_memory_multiplier").Value(jobResourceManagerInfo.FreeMemoryWatermarkMultiplier)
            .Item("cpu_to_vcpu_factor").Value(jobResourceManagerInfo.CpuToVCpuFactor)
            .Item("free_ports").Value(jobResourceManagerInfo.FreePorts)
        .EndMap();
    }

    void NotifyResourcesReleased(EResourcesConsumerType resourcesConsumerType, bool fullyReleased)
    {
        ResourcesReleased_.Fire(resourcesConsumerType, fullyReleased);
        for (const auto& callbacks : ResourcesConsumerCallbacks_) {
            callbacks.Fire();
        }
    }

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

    void CheckMemoryPressure()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        try {
            auto currentFaultCount = Bootstrap_->GetExecNodeBootstrap()->GetSlotManager()->GetMajorPageFaultCount();
            if (currentFaultCount != LastMajorPageFaultCount_) {
                HandleMajorPageFaultsRateIncrease(currentFaultCount);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Error getting information about major page faults");
        }
    }

    void HandleMajorPageFaultsRateIncrease(i64 currentFaultCount)
    {
        auto config = DynamicConfig_.Acquire()->MemoryPressureDetector;
        YT_LOG_DEBUG(
            "Increased rate of major page faults in node container detected (MajorPageFaultCount: %v -> %v, Delta: %v, Threshold: %v, Period: %v)",
            LastMajorPageFaultCount_,
            currentFaultCount,
            currentFaultCount - LastMajorPageFaultCount_,
            config->MajorPageFaultCountThreshold,
            config->CheckPeriod);

        if (config->Enabled &&
            currentFaultCount - LastMajorPageFaultCount_ > config->MajorPageFaultCountThreshold)
        {
            auto previousMemoryWatermarkMultiplier = FreeMemoryWatermarkMultiplier_;
            FreeMemoryWatermarkMultiplier_ = std::min(
                FreeMemoryWatermarkMultiplier_ + config->MemoryWatermarkMultiplierIncreaseStep,
                config->MaxMemoryWatermarkMultiplier);

            YT_LOG_DEBUG(
                "Increasing memory watermark multiplier "
                "(MemoryWatermarkMultiplier: %v -> %v, "
                "UpdatedFreeMemoryWatermark: %v, "
                "UserMemoryUsageTrackerLimit: %v, "
                "UserMemoryUsageTrackerUsed: %v, "
                "NodeMemoryUsageTrackerTotalFree: %v)",
                previousMemoryWatermarkMultiplier,
                FreeMemoryWatermarkMultiplier_,
                GetFreeMemoryWatermark(),
                UserMemoryUsageTracker_->GetLimit(),
                UserMemoryUsageTracker_->GetUsed(),
                NodeMemoryUsageTracker_->GetTotalFree());
        }

        LastMajorPageFaultCount_ = currentFaultCount;
    }

    //! Returns |true| if a acquiring with given #neededResources can succeed.
    //! Takes special care with ReplicationDataSize and RepairDataSize enabling
    //! an arbitrary large overdraft for the
    //! first acquiring.
    bool HasEnoughResources(
        const TJobResources& neededResources,
        const TJobResources& usedResources)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto totalResources = GetResourceLimits();
        auto spareResources = MakeNonnegative(totalResources - usedResources);
        // Allow replication/repair/merge data size overcommit.
        spareResources.ReplicationDataSize = InfiniteJobResources().ReplicationDataSize;
        spareResources.RepairDataSize = InfiniteJobResources().RepairDataSize;
        spareResources.MergeDataSize = InfiniteJobResources().MergeDataSize;
        return Dominates(spareResources, neededResources);
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobResourceManagerPtr IJobResourceManager::CreateJobResourceManager(IBootstrapBase* bootstrap)
{
    return New<IJobResourceManager::TImpl>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

IJobResourceManager::TResourceAcquiringContext::TResourceAcquiringContext(
    IJobResourceManager* resourceManager)
    : ResourceManagerImpl_(static_cast<IJobResourceManager::TImpl*>(resourceManager))
{
    ResourceManagerImpl_->OnResourceAcquiringStarted();
}

IJobResourceManager::TResourceAcquiringContext::~TResourceAcquiringContext()
{
    ResourceManagerImpl_->OnResourceAcquiringFinished();
}

bool IJobResourceManager::TResourceAcquiringContext::TryAcquireResourcesFor(const TResourceHolderPtr& resourceHolder) &
{
    try {
        if (!ResourceManagerImpl_->AcquireResourcesFor(resourceHolder)) {
            return false;
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(ex);
    }

    resourceHolder->OnResourcesAcquired();
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TResourceHolder::TResourceHolder(
    IJobResourceManager* jobResourceManager,
    EResourcesConsumerType resourceConsumerType,
    TLogger logger,
    const TJobResources& resources,
    const TJobResourceAttributes& resourceAttributes,
    int portCount)
    : Logger(std::move(logger))
    , ResourceManagerImpl_(static_cast<IJobResourceManager::TImpl*>(jobResourceManager))
    , PortCount_(portCount)
    , Resources_(resources)
    , ResourceAttributes_(resourceAttributes)
    , ResourcesConsumerType_(resourceConsumerType)
{
    ResourceManagerImpl_->OnResourceHolderCreated(Logger, Resources_);
}

TResourceHolder::~TResourceHolder()
{
    YT_LOG_DEBUG_IF(
        State_ != EResourcesState::Released,
        "Destruct unreleased resource holder (State: %v, Resources: %v)",
        State_,
        FormatResources(GetResourceUsage()));
    if (State_ != EResourcesState::Released) {
        ReleaseResources();
    }
}

void TResourceHolder::SetAcquiredResources(TAcquiredResources&& acquiredResources)
{
    auto guard = WriterGuard(ResourcesLock_);

    YT_VERIFY(State_ == EResourcesState::Waiting);

    Ports_ = std::move(acquiredResources.Ports);

    YT_VERIFY(PortCount_ == std::ssize(Ports_));

    acquiredResources.SystemMemoryGuard.ReleaseNoReclaim();
    acquiredResources.UserMemoryGuard.ReleaseNoReclaim();

    UserSlot_ = std::move(acquiredResources.UserSlot);
    GpuSlots_ = std::move(acquiredResources.GpuSlots);

    State_ = EResourcesState::Acquired;
}

void TResourceHolder::ReleaseCumulativeResources()
{
    auto usedSlotResources = ZeroJobResources();
    auto resources = GetResourceUsage();
    usedSlotResources.UserSlots = resources.UserSlots;
    usedSlotResources.Gpu = resources.Gpu;

    SetResourceUsage(usedSlotResources);
}

void TResourceHolder::ReleaseResources()
{
    TJobResources resources;
    {
        auto guard = ReaderGuard(ResourcesLock_);
        YT_VERIFY(State_ != EResourcesState::Released);
        resources = Resources_;
    }

    YT_LOG_FATAL_IF(UserSlot_ && UserSlot_->GetRefCount() > 1,
        "User slot leaked (RefCount: %v)",
        UserSlot_->GetRefCount());

    YT_LOG_FATAL_IF(UserSlot_ && resources.UserSlots != 1,
        "User slot not matched with UserSlots (UserSlotExist: %v, UserSlots: %v)",
        UserSlot_ != nullptr,
        resources.UserSlots);

    YT_LOG_FATAL_IF(std::ssize(GpuSlots_) > resources.Gpu,
        "GPU slots not matched with Gpu");

    YT_LOG_DEBUG("Reset resource holder slots");

    UserSlot_.Reset();
    GpuSlots_.clear();

    auto guard = WriterGuard(ResourcesLock_);
    ResourceManagerImpl_->OnResourcesReleased(
        ResourcesConsumerType_,
        Logger,
        Resources_,
        GetPorts(),
        /*resourceHolderStarted*/ State_ == EResourcesState::Acquired);
    State_ = EResourcesState::Released;
    Resources_ = ZeroJobResources();
}

const std::vector<int>& TResourceHolder::GetPorts() const noexcept
{
    return Ports_;
}

TJobResources TResourceHolder::SetResourceUsage(TJobResources newResourceUsage)
{
    auto guard = WriterGuard(ResourcesLock_);

    YT_LOG_FATAL_IF(
        State_ != EResourcesState::Acquired,
        "Resources should be setted only while resource is acquired (CurrentState: %v, NewResourceUsage: %v)",
        State_,
        FormatResources(newResourceUsage));

    auto resourceDelta = newResourceUsage - Resources_;
    ResourceManagerImpl_->OnResourcesUpdated(ResourcesConsumerType_, GetLogger(), resourceDelta);

    Resources_ = newResourceUsage;

    return resourceDelta;
}

TJobResources TResourceHolder::ChangeCumulativeResourceUsage(TJobResources resourceUsageDelta)
{
    auto guard = WriterGuard(ResourcesLock_);

    YT_LOG_FATAL_IF(
        State_ != EResourcesState::Acquired,
        "Resources should be changed only while resource is acquired (CurrentState: %v, ResourceUsageDelta: %v)",
        State_,
        FormatResources(resourceUsageDelta));

    ResourceManagerImpl_->OnResourcesUpdated(ResourcesConsumerType_, GetLogger(), resourceUsageDelta);
    Resources_ += resourceUsageDelta;

    return resourceUsageDelta;
}

TJobResources TResourceHolder::GetResourceLimits() const noexcept
{
    return ResourceManagerImpl_->GetResourceLimits();
}

TJobResources TResourceHolder::GetResourceUsage() const noexcept
{
    auto guard = ReaderGuard(ResourcesLock_);
    return Resources_;
}

const TJobResourceAttributes& TResourceHolder::GetResourceAttributes() const noexcept
{
    return ResourceAttributes_;
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
    ISlotPtr&& userSlot,
    std::vector<ISlotPtr>&& gpuSlots,
    std::vector<int> ports) noexcept
    : UserMemoryGuard(std::move(userMemoryGuard))
    , SystemMemoryGuard(std::move(systemMemoryGuard))
    , UserSlot(std::move(userSlot))
    , GpuSlots(std::move(gpuSlots))
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
