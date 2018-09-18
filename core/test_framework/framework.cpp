#include <yt/core/test_framework/framework.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fiber.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/format.h>

#include <util/random/random.h>

#include <util/string/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomFileName(const char* prefix)
{
    return Format("%s-%016" PRIx64 "-%016" PRIx64,
        prefix,
        MicroSeconds(),
        RandomNumber<ui64>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace testing {

using namespace NYT;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

Matcher<TStringBuf>::Matcher(const TString& s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<TStringBuf>::Matcher(const char* s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<TStringBuf>::Matcher(TStringBuf s)
{
    *this = Eq(s);
}

////////////////////////////////////////////////////////////////////////////////

Matcher<const TString&>::Matcher(const TString& s)
{
    *this = Eq(s);
}

Matcher<const TString&>::Matcher(const char* s)
{
    *this = Eq(TString(s));
}

Matcher<TString>::Matcher(const TString& s)
{
    *this = Eq(s);
}

Matcher<TString>::Matcher(const char* s)
{
    *this = Eq(TString(s));
}

////////////////////////////////////////////////////////////////////////////////

void RunAndTrackFiber(TClosure closure)
{
    auto queue = New<TActionQueue>("Main");
    auto invoker = queue->GetInvoker();

    auto promise = NewPromise<TFiberPtr>();

    BIND([invoker, promise, closure] () mutable {
        // NB: Make sure TActionQueue does not keep a strong reference to this fiber by forcing a yield.
        SwitchTo(invoker);
        promise.Set(GetCurrentScheduler()->GetCurrentFiber());
        closure.Run();
    })
    .Via(invoker)
    .Run();

    auto strongFiber = promise.Get().ValueOrThrow();
    auto weakFiber = MakeWeak(strongFiber);

    promise.Reset();
    strongFiber.Reset();

    Y_ASSERT(!promise);
    Y_ASSERT(!strongFiber);

    auto startedAt = TInstant::Now();
    while (weakFiber.Lock()) {
        if (TInstant::Now() - startedAt > TDuration::Seconds(5)) {
            GTEST_FAIL() << "Probably stuck.";
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }

    queue->Shutdown();

    SUCCEED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

void PrintTo(const TString& string, ::std::ostream* os)
{
    ::testing::internal::PrintTo(string.c_str(), os);
}

void PrintTo(TStringBuf string, ::std::ostream* os)
{
    ::testing::internal::PrintTo(string.c_str(), os);
}

////////////////////////////////////////////////////////////////////////////////
