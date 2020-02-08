#include "antiaffinity_allocator.h"

#include "pod.h"

#include <yp/client/api/proto/data_model.pb.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetVacancyLimit(const NClient::NApi::NProto::TAntiaffinityConstraint& constraint)
{
    if (constraint.max_pods() < 1) {
        return 1;
    }
    if (constraint.max_pods() >= std::numeric_limits<int>::max()) {
        return std::numeric_limits<int>::max();
    }
    return static_cast<int>(constraint.max_pods());
}

int GetCommonVacancyLimit(
    const std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>& constraints)
{
    auto result = std::numeric_limits<int>::max();
    for (const auto& constraint : constraints) {
        if (!constraint.pod_group_id_path()) {
            result = std::min(result, GetVacancyLimit(constraint));
        }
    }
    return result;
}

THashMap<NYPath::TYPath, int> GetGroupIdPathVacancyLimit(
    const std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>& constraints)
{
    THashMap<NYPath::TYPath, int> groupIdPathVacancyLimit;
    for (const auto& constraint : constraints) {
        if (constraint.pod_group_id_path()) {
            auto it = groupIdPathVacancyLimit.find(constraint.pod_group_id_path());
            if (it == groupIdPathVacancyLimit.end()) {
                YT_VERIFY(groupIdPathVacancyLimit.emplace(
                    constraint.pod_group_id_path(),
                    GetVacancyLimit(constraint)).second);
            } else {
                it->second = std::min(it->second, GetVacancyLimit(constraint));
            }
        }
    }
    return groupIdPathVacancyLimit;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TAntiaffinityVacancyAllocator::TAntiaffinityVacancyAllocator(
    const std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>& constraints)
    : CommonVacancyLimit_(GetCommonVacancyLimit(constraints))
    , GroupIdPathVacancyLimit_(GetGroupIdPathVacancyLimit(constraints))
    , Blocked_(false)
    , CommonVacancyCount_(0)
{ }

bool TAntiaffinityVacancyAllocator::CanAllocate(const TPod* pod) const
{
    if (Blocked_) {
        return false;
    }
    return GetVacancyCount(pod) > 0;
}

int TAntiaffinityVacancyAllocator::GetVacancyCount(const TPod* pod) const
{
    // Result for the blocked antiaffinity allocator cannot be properly defined.
    YT_VERIFY(!Blocked_);

    int vacancyCount = CommonVacancyLimit_ - CommonVacancyCount_;

    for (const auto& [groupIdPath, vacancyLimit] : GroupIdPathVacancyLimit_) {
        auto groupIdOrError = pod->GetAntiaffinityGroupId(groupIdPath);

        auto groupId = std::move(groupIdOrError).ValueOrThrow();
        if (!groupId) {
            continue;
        }

        auto key = std::make_pair(groupIdPath, std::move(groupId));
        auto it = GroupVacancyCount_.find(key);
        if (it == GroupVacancyCount_.end()) {
            // NB! Do not add elements to the map to prevent excessive memory consumption
            // due to wasteful allocations per every unsuccessful scheduling iteration.
            vacancyCount = std::min(vacancyCount, vacancyLimit);
        } else {
            vacancyCount = std::min(vacancyCount, vacancyLimit - it->second);
        }
    }

    return vacancyCount;
}

void TAntiaffinityVacancyAllocator::Allocate(const TPod* pod)
{
    CommonVacancyCount_ += 1;

    for (const auto& [groupIdPath, vacancyLimit] : GroupIdPathVacancyLimit_) {
        auto groupIdOrError = pod->GetAntiaffinityGroupId(groupIdPath);
        if (!groupIdOrError.IsOK()) {
            Blocked_ = true;
            continue;
        }

        auto groupId = std::move(groupIdOrError).Value();
        if (!groupId) {
            continue;
        }

        auto key = std::make_pair(groupIdPath, std::move(groupId));
        auto it = GroupVacancyCount_.find(key);
        if (it == GroupVacancyCount_.end()) {
            it = GroupVacancyCount_.emplace(std::move(key), 0).first;
        }

        it->second += 1;
    }
}

bool TAntiaffinityVacancyAllocator::IsBlocked() const
{
    return Blocked_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
