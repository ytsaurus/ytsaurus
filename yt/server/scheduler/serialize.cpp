#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200243;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200243;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

