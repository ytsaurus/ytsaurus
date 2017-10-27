#include "serialize.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200957;
}

bool ValidateSnapshotVersion(int version)
{
    return version == 200957;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
