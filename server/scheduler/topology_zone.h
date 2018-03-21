#pragma once

#include "private.h"

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TTopologyZone
    : public NYT::TRefTracked<TTopologyZone>
{
public:
    TTopologyZone(
        TString key,
        TString value);

    const TString& GetKey() const;
    const TString& GetValue() const;

    using TPodSetToAntiaffinityVacancies = THashMap<TPodSet*, int>;
    DEFINE_BYREF_RW_PROPERTY(TPodSetToAntiaffinityVacancies, PodSetToAntiaffinityVacancies);

    bool CanAcquireAntiaffinityVacancy(const TPod* pod) const;
    void AcquireAntiaffinityVacancy(const TPod* pod);
    void ReleaseAntiaffinityVacancy(const TPod* pod);

private:
    const TString Key_;
    const TString Value_;

    int* GetAntiaffinityVacancies(const TPod* pod);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
