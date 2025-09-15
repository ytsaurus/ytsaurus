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
        version >= ToUnderlying(ESnapshotVersion::BumpTo_25_3) &&
        version <= ToUnderlying(GetCurrentSnapshotVersion())
        // COMPAT(coteeq): This snapshot is completely broken and cannot be loaded from.
        && version != ToUnderlying(ESnapshotVersion::RlsInOperations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
