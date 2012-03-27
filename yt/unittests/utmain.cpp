#include "stdafx.h"

#include <util/datetime/base.h>
#include <util/random/random.h>
#include <util/string/printf.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GenerateRandomFileName(const char* prefix)
{
    return Sprintf("%s-%016" PRIx64 "-%016" PRIx64,
        prefix,
        MicroSeconds(),
        RandomNumber<ui64>());
}

////////////////////////////////////////////////////////////////////////////////
 
} // namespace NYT

namespace testing {

////////////////////////////////////////////////////////////////////////////////

Matcher<const Stroka&>::Matcher(const Stroka& s)
{
    *this = Eq(s);
}

Matcher<const Stroka&>::Matcher(const char* s)
{
    *this = Eq(Stroka(s));
}

Matcher<Stroka>::Matcher(const Stroka& s)
{
    *this = Eq(s);
}

Matcher<Stroka>::Matcher(const char* s)
{
    *this = Eq(Stroka(s));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();

    // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
    TAsyncChangeLog::Shutdown();
    NLog::TLogManager::Get()->Shutdown();
    NProfiling::TProfilingManager::Get()->Shutdown();
    NRpc::TRpcManager::Get()->Shutdown();
    TDelayedInvoker::Shutdown();

    return rv;
}
