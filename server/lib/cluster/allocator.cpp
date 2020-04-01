#include "allocator.h"

#include "cluster.h"
#include "daemon_set.h"
#include "ip4_address_pool.h"
#include "node.h"
#include "pod.h"
#include "resource_capacities.h"

#include <yt/core/misc/error.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSingleInternetAddressAllocationContext
{
public:
    TSingleInternetAddressAllocationContext(
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest* request,
        TIP4AddressPool* pool)
        : Request_(request)
        , IP4AddressPool_(pool)
    { }

    bool TryAllocate(TAllocatorDiagnostics* diagnostics)
    {
        if (Request_->enable_internet() || !Request_->ip4_address_pool_id().empty()) {
            if (!IP4AddressPool_) {
                diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressIP4TunnelUnknownIP4AddressPool);
                return false;
            }
            if (IP4AddressPool_->AllocatedInternetAddressCount() + 1 > IP4AddressPool_->InternetAddressCount()) {
                diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressIP4TunnelCapacity);
                return false;
            }
            IP4AddressPool_->AllocatedInternetAddressCount() += 1;
            TemporarilyAllocated_ = true;
        }
        return true;
    }

    void Commit() noexcept
    {
        // Unset flag to indicate permanent allocation.
        TemporarilyAllocated_ = false;
    }

    ~TSingleInternetAddressAllocationContext() noexcept
    {
        if (TemporarilyAllocated_) {
            IP4AddressPool_->AllocatedInternetAddressCount() -= 1;
            TemporarilyAllocated_ = false;
        }
    }

private:
    const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest* const Request_;
    TIP4AddressPool* const IP4AddressPool_;
    bool TemporarilyAllocated_ = false;
};

class TInternetAddressesAllocationContext
{
public:
    explicit TInternetAddressesAllocationContext(TPod* pod)
    {
        auto& ip6AddressRequests = pod->IP6AddressRequests();
        for (size_t i = 0; i < ip6AddressRequests.GetSize(); ++i) {
            const auto& request = ip6AddressRequests.ProtoRequests()[i];
            auto* pool = ip6AddressRequests.IP4AddressPools().at(i);
            SingleAddressContexts_.emplace_back(&request, pool);
        }
    }

    bool TryAllocate(TAllocatorDiagnostics* diagnostics)
    {
        bool result = true;
        for (auto& context : SingleAddressContexts_) {
            if (!context.TryAllocate(diagnostics)) {
                result = false;
            }
        }
        return result;
    }

    void Commit() noexcept
    {
        for (auto& context : SingleAddressContexts_) {
            context.Commit();
        }
    }

private:
    std::vector<TSingleInternetAddressAllocationContext> SingleAddressContexts_;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeAllocationContext
{
public:
    TNodeAllocationContext(TNode* node, TPod* pod)
        : CpuResource_(node->CpuResource())
        , MemoryResource_(node->MemoryResource())
        , NetworkResource_(node->NetworkResource())
        , SlotResource_(node->SlotResource())
        , DiskResources_(node->DiskResources())
        , GpuResources_(node->GpuResources())
        , Node_(node)
        , Pod_(pod)
    { }

    TPod* GetPod() const
    {
        return Pod_;
    }

    bool TryAcquireIP6Addresses(TAllocatorDiagnostics* diagnostics)
    {
        for (const auto& addressRequest : Pod_->IP6AddressRequests().ProtoRequests()) {
            if (!Node_->HasIP6SubnetInVlan(addressRequest.vlan_id())) {
                diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressVlan);
                return false;
            }
        }
        return true;
    }

    bool TryAcquireIP6Subnets()
    {
        // NB! Assumming this resource has infinite capacity we do not need to acquire anything.
        for (const auto& subnetRequest : Pod_->IP6SubnetRequests()) {
            if (!Node_->HasIP6SubnetInVlan(subnetRequest.vlan_id())) {
                return false;
            }
        }
        return true;
    }

    bool TryAllocateAntiaffinityVacancies()
    {
        // NB! Just checking: actual allocation is deferred to the commit stage.
        return Node_->CanAllocateAntiaffinityVacancies(Pod_);
    }

    bool TryAllocateDaemonSets()
    {
        bool daemonSetsSatisfied = true;
        for (const auto& [daemonSet, daemonSetPod] : Node_->DaemonSetPods()) {
            if (daemonSet->GetPodSet() == Pod_->GetPodSet()) {
                // This is a daemon set pod, it's free to get allocated.
                return true;
            }
            if (daemonSetPod == nullptr && daemonSet->Spec().strong()) {
                // If there's at least one unsatisfied daemon set, defer everything to next
                // iteration.
                daemonSetsSatisfied = false;
            }
        }
        return daemonSetsSatisfied;
    }

    void Commit() noexcept
    {
        Node_->AllocateAntiaffinityVacancies(Pod_);

        Node_->CpuResource() = CpuResource_;
        Node_->MemoryResource() = MemoryResource_;
        Node_->NetworkResource() = NetworkResource_;
        Node_->SlotResource() = SlotResource_;
        Node_->DiskResources() = DiskResources_;
        Node_->GpuResources() = GpuResources_;
    }

    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, CpuResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, MemoryResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, NetworkResource);
    DEFINE_BYREF_RW_PROPERTY(THomogeneousResource, SlotResource);
    DEFINE_BYREF_RW_PROPERTY(TNode::TDiskResources, DiskResources);
    DEFINE_BYREF_RW_PROPERTY(TNode::TGpuResources, GpuResources);

private:
    TNode* const Node_;
    TPod* const Pod_;
};

////////////////////////////////////////////////////////////////////////////////

bool TryAllocateNodeResourses(
    TNodeAllocationContext* nodeAllocationContext,
    TAllocatorDiagnostics* diagnostics)
{
    bool result = true;

    if (!nodeAllocationContext->TryAcquireIP6Addresses(diagnostics)) {
        result = false;
    }

    if (!nodeAllocationContext->TryAcquireIP6Subnets()) {
        result = false;
        diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6Subnet);
    }

    if (!nodeAllocationContext->TryAllocateAntiaffinityVacancies()) {
        result = false;
        diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Antiaffinity);
    }

    const auto& resourceRequests = nodeAllocationContext->GetPod()->ResourceRequests();

    if (resourceRequests.vcpu_guarantee() > 0) {
        if (!nodeAllocationContext->CpuResource().TryAllocate(MakeCpuCapacities(resourceRequests.vcpu_guarantee()))) {
            result = false;
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Cpu);
        }
    }

    if (resourceRequests.memory_limit() > 0) {
        if (!nodeAllocationContext->MemoryResource().TryAllocate(MakeMemoryCapacities(resourceRequests.memory_limit()))) {
            result = false;
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Memory);
        }
    }

    if (resourceRequests.network_bandwidth_guarantee() > 0) {
        if (!nodeAllocationContext->NetworkResource().TryAllocate(MakeNetworkCapacities(resourceRequests.network_bandwidth_guarantee()))) {
            result = false;
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Network);
        }
    }

    if (resourceRequests.slot() > 0) {
        if (!nodeAllocationContext->SlotResource().TryAllocate(MakeSlotCapacities(resourceRequests.slot()))) {
            result = false;
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Slot);
        }
    }

    bool allDiskVolumeRequestsSatisfied = true;
    for (const auto& volumeRequest : nodeAllocationContext->GetPod()->DiskVolumeRequests()) {
        const auto& storageClass = volumeRequest.storage_class();
        auto policy = GetDiskVolumeRequestPolicy(volumeRequest);
        auto capacities = GetDiskVolumeRequestCapacities(volumeRequest);
        bool exclusive = GetDiskVolumeRequestExclusive(volumeRequest);
        bool satisfied = false;
        for (auto& diskResource : nodeAllocationContext->DiskResources()) {
            if (diskResource.TryAllocate(exclusive, storageClass, policy, capacities)) {
                satisfied = true;
                break;
            }
        }
        if (!satisfied) {
            allDiskVolumeRequestsSatisfied = false;
            break;
        }
    }

    if (!allDiskVolumeRequestsSatisfied) {
        result = false;
        diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Disk);
    }

    bool allGpuRequestsSatisfied = true;
    const auto& gpuRequests = nodeAllocationContext->GetPod()->GpuRequests();
    for (const auto& gpuRequest : gpuRequests) {
        const auto capacities = GetGpuRequestCapacities(gpuRequest);
        bool satisfied = false;
        for (auto& gpuResource : nodeAllocationContext->GpuResources()) {
            if (gpuResource.GetModel() != gpuRequest.model()) {
                continue;
            }
            if (gpuRequest.has_min_memory()
                && gpuResource.GetTotalMemory() < gpuRequest.min_memory())
            {
                continue;
            }
            if (gpuRequest.has_max_memory()
                && gpuResource.GetTotalMemory() > gpuRequest.max_memory())
            {
                continue;
            }
            if (gpuResource.TryAllocate(capacities)) {
                satisfied = true;
                break;
            }
        }
        if (!satisfied) {
            allGpuRequestsSatisfied = false;
            break;
        }
    }

    if (!allGpuRequestsSatisfied) {
        result = false;
        diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::Gpu);
    }

    if (!nodeAllocationContext->TryAllocateDaemonSets()) {
        result = false;
        diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::DaemonSet);
    }

    return result;
}

bool TryAllocate(
    TInternetAddressesAllocationContext* internetAllocationContext,
    TNodeAllocationContext* nodeAllocationContext,
    TAllocatorDiagnostics* diagnostics)
{
    bool result = true;

    if (!internetAllocationContext->TryAllocate(diagnostics)) {
        result = false;
    }

    if (!TryAllocateNodeResourses(nodeAllocationContext, diagnostics)) {
        result = false;
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TAllocatorDiagnostics::RegisterUnsatisfiedConstraint(EAllocatorConstraintKind constraintKind)
{
    ++UnsatisfiedConstraintCounters_[constraintKind];
}

const TAllocatorConstraintCounters& TAllocatorDiagnostics::GetUnsatisfiedConstraintCounters() const
{
    return UnsatisfiedConstraintCounters_;
}

////////////////////////////////////////////////////////////////////////////////

void TAllocator::Allocate(TNode* node, TPod* pod)
{
    TInternetAddressesAllocationContext internetAllocationContext(pod);
    TNodeAllocationContext nodeAllocationContext(node, pod);
    if (!TryAllocate(&internetAllocationContext, &nodeAllocationContext, &Diagnostics_)) {
        THROW_ERROR_EXCEPTION("Could not allocate resources for pod %Qv on node %Qv",
            pod->GetId(),
            node->GetId());
    }
    internetAllocationContext.Commit();
    nodeAllocationContext.Commit();
}

bool TAllocator::CanAllocate(TNode* node, TPod* pod)
{
    TInternetAddressesAllocationContext internetAllocationContext(pod);
    TNodeAllocationContext nodeAllocationContext(node, pod);
    return TryAllocate(&internetAllocationContext, &nodeAllocationContext, &Diagnostics_);
}

const TAllocatorDiagnostics& TAllocator::GetDiagnostics() const
{
    return Diagnostics_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
