#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<ETransactionSupervisorReign>::GetMaxValue());
}

bool ValidateSnapshotReign(NHydra::TReign reign)
{
    for (auto value : TEnumTraits<ETransactionSupervisorReign>::GetDomainValues()) {
        if (ToUnderlying(value) == reign) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
