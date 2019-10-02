#pragma once

#include "public.h"

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

//! Allocates vacancies infered from pod set constraints within one topology zone.
class TAntiaffinityVacancyAllocator
{
public:
    explicit TAntiaffinityVacancyAllocator(
        const std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>& constraints);

    //! Checks whether antiaffinity constraints have a vacancy for the pod
    //! both within antiaffinity groups this pod belongs to and without any group.
    bool CanAllocate(const TPod* pod) const;

    //! Allocates vacancies for pod.
    //! Ignores pod validation errors, does not perform overcommit check.
    void Allocate(const TPod* pod);

private:
    const int CommonVacancyLimit_;
    const THashMap<NYPath::TYPath, int> GroupIdPathVacancyLimit_;

    bool Blocked_;

    int CommonVacancyCount_;
    // Indexed by group id attribute path and value.
    THashMap<std::pair<NYPath::TYPath, TString>, int> GroupVacancyCount_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
