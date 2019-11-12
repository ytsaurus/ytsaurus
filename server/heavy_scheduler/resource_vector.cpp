#include "resource_vector.h"

#include <yp/server/lib/cluster/network_module.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/resource_capacities.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

////////////////////////////////////////////////////////////////////////////////

TResourceVector::TResourceVector(std::array<TValue, DimensionCount> values)
    : Values_(values)
{ }

TResourceVector::TValue& TResourceVector::operator [] (size_t i)
{
    return Values_[i];
}

TResourceVector::TValue TResourceVector::operator [] (size_t i) const
{
    return Values_[i];
}

////////////////////////////////////////////////////////////////////////////////

TResourceVector& operator -= (TResourceVector& lhs, const TResourceVector& rhs)
{
    for (size_t i = 0; i < TResourceVector::DimensionCount; ++i) {
        lhs[i] = (lhs[i] < rhs[i]) ? 0 : lhs[i] - rhs[i];
    }
    return lhs;
}

TResourceVector operator - (TResourceVector lhs, const TResourceVector& rhs)
{
    lhs -= rhs;
    return lhs;
}

TResourceVector& operator += (TResourceVector& lhs, const TResourceVector& rhs)
{
    for (size_t i = 0; i < TResourceVector::DimensionCount; ++i) {
        lhs[i] += rhs[i];
    }
    return lhs;
}

TResourceVector operator + (TResourceVector lhs, const TResourceVector& rhs)
{
    lhs += rhs;
    return lhs;
}

bool operator >= (const TResourceVector& lhs, const TResourceVector& rhs)
{
    for (size_t i = 0; i < TResourceVector::DimensionCount; ++i) {
        if (lhs[i] < rhs[i]) {
            return false;
        }
    }
    return true;
}

bool operator < (const TResourceVector& lhs, const TResourceVector& rhs)
{
    return !(lhs >= rhs);
}

////////////////////////////////////////////////////////////////////////////////

TResourceVector GetResourceRequestVector(TPod* pod)
{
    const auto& resourceRequests = pod->ResourceRequests();
    return TResourceVector({
        pod->GetInternetAddressRequestCount(),
        resourceRequests.vcpu_guarantee(),
        resourceRequests.memory_limit(),
        resourceRequests.network_bandwidth_guarantee(),
        resourceRequests.slot(),
        pod->GetDiskRequestTotalCapacity(HddStorageClass),
        pod->GetDiskRequestTotalCapacity(SsdStorageClass)});
}

TResourceVector GetFreeResourceVector(TNode* node)
{
    ui64 internetAddressCount = 0;
    if (auto* module = node->GetNetworkModule()) {
        if (module->InternetAddressCount() >= module->AllocatedInternetAddressCount()) {
            internetAddressCount = module->InternetAddressCount() - module->AllocatedInternetAddressCount();
        }
    }
    return TResourceVector({
        internetAddressCount,
        GetCpuCapacity(node->CpuResource().GetFreeCapacities()),
        GetMemoryCapacity(node->MemoryResource().GetFreeCapacities()),
        GetNetworkBandwidth(node->NetworkResource().GetFreeCapacities()),
        GetSlotCapacity(node->SlotResource().GetFreeCapacities()),
        node->GetDiskResourceTotalFreeCapacity(HddStorageClass),
        node->GetDiskResourceTotalFreeCapacity(SsdStorageClass)});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
