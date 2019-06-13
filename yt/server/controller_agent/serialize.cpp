#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return ToUnderlying(TEnumTraits<ESnapshotVersion>::GetDomainValues().Back());
}

bool ValidateSnapshotVersion(int version)
{
    return version >= ToUnderlying(ESnapshotVersion::JobMetricsAggregationType) && version <= GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
