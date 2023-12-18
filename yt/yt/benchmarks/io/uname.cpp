#include "uname.h"

#include <library/cpp/yt/assert/assert.h>

#include <sys/utsname.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

TString Uname()
{
    struct utsname buf;
    YT_VERIFY(uname(&buf) == 0);
    return TString(buf.sysname) + " " +
        buf.nodename + " " +
        buf.release + " " +
        buf.version + " " +
        buf.machine + " " +
        buf.domainname;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
