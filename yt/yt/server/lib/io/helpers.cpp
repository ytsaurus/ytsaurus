#include "helpers.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

bool ShouldUseDirectIO(EDirectIOPolicy policy, bool userRequestedDirectIO)
{
    if (policy == EDirectIOPolicy::Never) {
        return false;
    }

    if (policy == EDirectIOPolicy::Always) {
        return true;
    }

    return userRequestedDirectIO;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
