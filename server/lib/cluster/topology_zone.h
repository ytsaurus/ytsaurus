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

    //! Returns exact vacancy count if corresponding antiaffinity allocator is initialized
    //! (e.g. in case of overcommit), otherwise just returns 1 as a lower bound.
    //! Returns nullopt if result cannot be determined (e.g. in case of previous errors).
    std::optional<int> TryEstimateAntiaffinityVacancyCount(const TPod* pod) const;

private:
    const TString Key_;
    const TString Value_;

    THashMap<const TPodSet*, TAntiaffinityVacancyAllocator> PodSetVacancyAllocators_;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TTopologyZone& zone,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
