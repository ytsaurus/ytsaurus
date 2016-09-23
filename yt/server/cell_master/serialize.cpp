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
        version == 125 ||
        version == 200 ||
        version == 201 ||
        version == 202 ||
        version == 203 ||
        version == 204 ||
        version == 205 ||
        version == 206 ||
        version == 207 ||
        version == 208 ||
        version == 209 ||
        version == 210 ||
        version == 211 ||
        version == 212 ||
        version == 213 ||
        version == 300 ||
        version == 301 ||
        version == 302 ||
        version == 303 ||
        version == 304 ||
        version == 350 ||
        version == 351 ||
        version == 352 ||
        version == 400 ||
        version == 401 ||
        version == 402 ||
        version == 403 ||
        version == 404 ||
        version == 405 ||
        version == 406 ||
        version == 500;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
