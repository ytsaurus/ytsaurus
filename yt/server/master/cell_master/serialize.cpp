#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return ToUnderlying(TEnumTraits<EMasterSnapshotVersion>::GetMaxValue());
}

bool ValidateSnapshotVersion(int version)
{
    for (auto v : TEnumTraits<EMasterSnapshotVersion>::GetDomainValues()) {
        if (v == version) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
