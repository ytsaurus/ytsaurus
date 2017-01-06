#include "serialize.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 500;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 354 ||
        version == 355 ||
        version == 356 ||
        version == 400 ||
        version == 401 ||
        version == 401 ||
        version == 402 ||
        version == 403 ||
        version == 500;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
