#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 300010;
}

bool ValidateSnapshotVersion(int version)
{
    return version >= 300010 && version <= GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
