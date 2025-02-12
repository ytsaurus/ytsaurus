#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion()
{
    return TEnumTraits<ESnapshotVersion>::GetDomainValues().back();
}

bool ValidateSnapshotVersion(int version)
{
    // NB: Version can be not valid enum value, so we do not cast version to enum here.
    return
        version >= ToUnderlying(ESnapshotVersion::GroupedNeededResources) &&
        version <= ToUnderlying(GetCurrentSnapshotVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
