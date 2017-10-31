#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200926;
}

bool ValidateSnapshotVersion(int version)
{
    return version >= 200925;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
