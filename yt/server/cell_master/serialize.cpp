#include "serialize.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 400;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 354 ||
        version == 355 ||
        version == 356 ||
        version == 400;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
