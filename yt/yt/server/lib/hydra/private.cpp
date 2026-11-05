#include "private.h"
#include "config.h"

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

bool IsSystemMutationType(const std::string& mutationType)
{
    return mutationType == NHydra::HeartbeatMutationType ||
        mutationType == ExitReadOnlyMutationType ||
        mutationType == EnterReadOnlyMutationType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
