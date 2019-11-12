#include "allocator.h"

#include "cluster.h"
#include "network_module.h"
#include "node.h"
#include "pod.h"
#include "resource_capacities.h"

#include <yt/core/misc/error.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TInternetAddressAllocationContext
{
public:
    explicit TInternetAddressAllocationContext(TNetworkModule* networkModule)
        : NetworkModule_(networkModule)
    { }

    bool TryAllocate(int allocationSize, TAllocatorDiagnostics* diagnostics)
    {
        ReleaseAddresses();
        return TryAcquireAddresses(allocationSize, diagnostics);
    }

    void Commit()
    {
        AllocationSize_ = 0;
    }

    ~TInternetAddressAllocationContext()
    {
        ReleaseAddresses();
    }

private:
    TNetworkModule* const NetworkModule_;

    int AllocationSize_ = 0;

private:
    void ReleaseAddresses()
    {
        if (AllocationSize_ > 0) {
            YT_VERIFY(NetworkModule_);
            YT_VERIFY(NetworkModule_->AllocatedInternetAddressCount() >= AllocationSize_);
            NetworkModule_->AllocatedInternetAddressCount() -= AllocationSize_;
            AllocationSize_ = 0;
        }
    }

    bool TryAcquireAddresses(int allocationSize, TAllocatorDiagnostics* diagnostics)
    {
        YT_VERIFY(allocationSize >= 0);

        if (allocationSize == 0) {
            return true;
        }

        if (!NetworkModule_) {
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressIP4TunnelUnknownNetworkModule);
            return false;
        }

        if (NetworkModule_->AllocatedInternetAddressCount() + allocationSize > NetworkModule_->InternetAddressCount()) {
            diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressIP4TunnelCapacity);
            return false;
        }

        NetworkModule_->AllocatedInternetAddressCount() += allocationSize;
        AllocationSize_ = allocationSize;

        return true;
    }
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
        , InternetAddressAllocationContext_(Node_->GetNetworkModule())
    { }

    bool TryAcquireIP6Addresses(TAllocatorDiagnostics* diagnostics)
    {
        bool result = true;
        for (const auto& addressRequest : Pod_->IP6AddressRequests()) {
            if (!Node_->HasIP6SubnetInVlan(addressRequest.vlan_id()) && result) {
                diagnostics->RegisterUnsatisfiedConstraint(EAllocatorConstraintKind::IP6AddressVlan);
                result = false;
            }
        }
        auto internetAddressCount = Pod_->GetInternetAddressRequestCount();
        if (!InternetAddressAllocationContext_.TryAllocate(internetAddressCount, diagnostics)) {
            result = false;
        }
        return result;
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

    void Commit()
    {
        Node_->AllocateAntiaffinityVacancies(Pod_);

        Node_->CpuResource() = CpuResource_;
        Node_->MemoryResource() = MemoryResource_;
        Node_->NetworkResource() = NetworkResource_;
        Node_->SlotResource() = SlotResource_;
        Node_->DiskResources() = DiskResources_;
        Node_->GpuResources() = GpuResources_;

        InternetAddressAllocationContext_.Commit();
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

    TInternetAddressAllocationContext InternetAddressAllocationContext_;
};

////////////////////////////////////////////////////////////////////////////////

bool TryAllocate(
    TNodeAllocationContext* nodeAllocationContext,
    TPod* pod,
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

    const auto& resourceRequests = pod->ResourceRequests();

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
    for (const auto& volumeRequest : pod->DiskVolumeRequests()) {
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
    const auto& gpuRequests = pod->GpuRequests();
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
    TNodeAllocationContext allocationContext(node, pod);
    if (!TryAllocate(&allocationContext, pod, &Diagnostics_)) {
        THROW_ERROR_EXCEPTION("Could not allocate resources for pod %Qv on node %Qv",
            pod->GetId(),
            node->GetId());
    }
    allocationContext.Commit();
}

bool TAllocator::CanAllocate(TNode* node, TPod* pod)
{
    TNodeAllocationContext allocationContext(node, pod);
    return TryAllocate(&allocationContext, pod, &Diagnostics_);
}

const TAllocatorDiagnostics& TAllocator::GetDiagnostics() const
{
    return Diagnostics_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
