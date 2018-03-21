#include "topology_zone.h"
#include "pod_set.h"
#include "pod.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

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

bool TTopologyZone::CanAcquireAntiaffinityVacancy(const TPod* pod) const
{
    auto it = PodSetToAntiaffinityVacancies_.find(pod->GetPodSet());
    return it == PodSetToAntiaffinityVacancies_.end() || it->second > 0;
}

void TTopologyZone::AcquireAntiaffinityVacancy(const TPod* pod)
{
    --(*GetAntiaffinityVacancies(pod));
}

void TTopologyZone::ReleaseAntiaffinityVacancy(const TPod* pod)
{
    ++(*GetAntiaffinityVacancies(pod));
}

int* TTopologyZone::GetAntiaffinityVacancies(const TPod* pod)
{
    auto* podSet = pod->GetPodSet();
    auto it = PodSetToAntiaffinityVacancies_.find(podSet);
    if (it == PodSetToAntiaffinityVacancies_.end()) {
        int vacancies = std::numeric_limits<int>::max();
        for (const auto& constraint : podSet->AntiaffinityConstraints()) {
            if (constraint.key() == Key_ && constraint.max_pods() < std::numeric_limits<int>::max()) {
                vacancies = std::min(vacancies, static_cast<int>(constraint.max_pods()));
            }
        }
        it = PodSetToAntiaffinityVacancies_.emplace(podSet, vacancies).first;
    }
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

