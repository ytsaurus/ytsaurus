#include "uname.h"

#include <library/cpp/yt/assert/assert.h>

#include <sys/utsname.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

std::string Uname()
{
    struct utsname buf;
    YT_VERIFY(uname(&buf) == 0);
    return std::string(buf.sysname) + " " +
        buf.nodename + " " +
        buf.release + " " +
        buf.version + " " +
        buf.machine + " " +
        buf.domainname;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
