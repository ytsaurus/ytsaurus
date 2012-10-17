#include "stdafx.h"

#include <ytlib/bus/tcp_dispatcher.h>
#include <ytlib/rpc/rpc_dispatcher.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/meta_state/async_change_log.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <util/datetime/base.h>
#include <util/random/random.h>
#include <util/string/printf.h>
#include <util/string/escape.h>

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

Matcher<const TStringBuf&>::Matcher(const Stroka& s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<const TStringBuf&>::Matcher(const char* s)
{
    *this = Eq(TStringBuf(s));
}

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

void PrintTo(const Stroka& string, ::std::ostream* os)
{
    *os << string.c_str();
}

void PrintTo(const TStringBuf& string, ::std::ostream* os)
{
    *os << Stroka(string);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();

    // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp.
    NYT::NMetaState::TAsyncChangeLog::Shutdown();
    NYT::NLog::TLogManager::Get()->Shutdown();
    NYT::NProfiling::TProfilingManager::Get()->Shutdown();
    NYT::NBus::TTcpDispatcher::Get()->Shutdown();
    NYT::NRpc::TRpcDispatcher::Get()->Shutdown();
    NYT::NChunkClient::TDispatcher::Get()->Shutdown();
    NYT::TDelayedInvoker::Shutdown();

    return rv;
}
