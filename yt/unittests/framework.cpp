#include "stdafx.h"
#include "framework.h"

#include <core/misc/format.h>

#include <core/actions/future.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>

#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GenerateRandomFileName(const char* prefix)
{
    return Format("%s-%016" PRIx64 "-%016" PRIx64,
        prefix,
        MicroSeconds(),
        RandomNumber<ui64>());
}

TShadowingAtExitManager::TShadowingAtExitManager()
    : TAtExitManager(true)
{ }

TShadowingAtExitManager::~TShadowingAtExitManager()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace testing {

using namespace NYT;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

Matcher<const TStringBuf&>::Matcher(const Stroka& s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<const TStringBuf&>::Matcher(const char* s)
{
    *this = Eq(TStringBuf(s));
}

Matcher<const TStringBuf&>::Matcher(const TStringBuf& s)
{
    *this = Eq(s);
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

void TrackedVia(TClosure closure)
{
    auto queue = New<TActionQueue>("Main");
    auto invoker = queue->GetInvoker();

    auto promise = NewPromise<TFiberPtr>();

    BIND([promise, closure] () mutable {
        promise.Set(GetCurrentScheduler()->GetCurrentFiber());
        closure.Run();
    })
    .Via(invoker)
    .Run();

    auto strongFiber = promise.Get();
    auto weakFiber = MakeWeak(strongFiber);

    promise.Reset();
    strongFiber.Reset();

    YASSERT(!promise);
    YASSERT(!strongFiber);

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

void PrintTo(const Stroka& string, ::std::ostream* os)
{
    *os << string.c_str();
}

void PrintTo(const TStringBuf& string, ::std::ostream* os)
{
    *os << Stroka(string);
}

////////////////////////////////////////////////////////////////////////////////
