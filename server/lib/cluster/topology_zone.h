#pragma once

#include "public.h"

#include "antiaffinity_allocator.h"

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TTopologyZone
    : public TRefTracked<TTopologyZone>
{
public:
    TTopologyZone(
        TString key,
        TString value);

    const TString& GetKey() const;
    const TString& GetValue() const;

    bool CanAllocateAntiaffinityVacancies(const TPod* pod) const;
    void AllocateAntiaffinityVacancies(const TPod* pod);

private:
    const TString Key_;
    const TString Value_;

    THashMap<const TPodSet*, TAntiaffinityVacancyAllocator> PodSetVacancyAllocators_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
