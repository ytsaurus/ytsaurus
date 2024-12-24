#include <library/cpp/yt/exception/exception.h>

#include <library/cpp/yt/string/format.h>

#include <util/system/compiler.h>

using namespace NYT;

Y_WEAK extern "C" void _ZNSt4__y126__libcpp_assertion_handlerEPKciS1_S1_(
    char const* file,
    int line,
    char const* expression,
    char const* message)
{
    throw TSimpleException(Format("%v:%v: libc++ assertion %qv failed: %v",
        file,
        line,
        expression,
        message));
}
