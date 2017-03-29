#include "serialize.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 513;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 354 ||
        version == 355 ||
        version == 356 ||
        version == 400 ||
        version == 401 ||
        version == 402 ||
        version == 403 ||
        version == 500 ||
        version == 501 ||
        version == 502 ||
        version == 503 ||
        version == 504 ||
        version == 505 ||
        version == 506 ||
        version == 507 ||
        version == 508 ||
        version == 509 ||
        version == 510 ||
        version == 511 ||
        version == 512 ||
        version == 513;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
