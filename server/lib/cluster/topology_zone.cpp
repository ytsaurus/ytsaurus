#include "topology_zone.h"

#include "pod.h"
#include "pod_set.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TTopologyZone::TTopologyZone(
    TString key,
    TString value)
    : Key_(std::move(key))
    , Value_(std::move(value))
{ }

const TString& TTopologyZone::GetKey() const
{
    return Key_;
}

const TString& TTopologyZone::GetValue() const
{
    return Value_;
}

bool TTopologyZone::CanAllocateAntiaffinityVacancies(const TPod* pod) const
{
    // NB! Do not add elements to the map to prevent excessive memory consumption
    // due to wasteful allocations per every unsuccessful scheduling iteration.
    auto it = PodSetVacancyAllocators_.find(pod->GetPodSet());

    // NB! Vacancy limit is clamped to the range [1, \infty), so non-initialized allocator
    // is supposed to always have at least one vacancy for the pod.
    return it == PodSetVacancyAllocators_.end() || it->second.CanAllocate(pod);
}

void TTopologyZone::AllocateAntiaffinityVacancies(const TPod* pod)
{
    const auto* podSet = pod->GetPodSet();
    auto it = PodSetVacancyAllocators_.find(podSet);
    if (it == PodSetVacancyAllocators_.end()) {
        std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> topologyZoneConstraints;
        topologyZoneConstraints.reserve(podSet->AntiaffinityConstraints().size());
        for (const auto& constraint : podSet->AntiaffinityConstraints()) {
            if (constraint.key() == Key_) {
                topologyZoneConstraints.push_back(constraint);
            }
        }
        it = PodSetVacancyAllocators_.emplace(
            podSet,
            TAntiaffinityVacancyAllocator(topologyZoneConstraints)).first;
    }
    it->second.Allocate(pod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
