#include "shutdown.h"

#include <ytlib/shutdown.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

void RegisterShutdown()
{
    static bool registered = false;
    if (!registered) {
        registered = true;
        Py_AtExit(NYT::Shutdown);
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

