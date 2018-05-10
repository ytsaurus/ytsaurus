#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 300003;
}

bool ValidateSnapshotVersion(int version)
{
    return version >= 300002 && version <= GetCurrentSnapshotVersion();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
