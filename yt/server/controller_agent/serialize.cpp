#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200384;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200384;
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

