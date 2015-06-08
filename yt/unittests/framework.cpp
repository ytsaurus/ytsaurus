#include "stdafx.h"
#include "framework.h"

#include <core/misc/format.h>

#include <core/actions/future.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>

#include <core/logging/log_manager.h>
#include <core/logging/config.h>

#include <util/random/random.h>

#include <util/string/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void ConfigureLogging(
    const char* logLevelFromEnv,
    const char* logExcludeCategoriesFromEnv,
    const char* logIncludeCategoriesFromEnv)
{
    if (!logLevelFromEnv && !logExcludeCategoriesFromEnv && !logIncludeCategoriesFromEnv) {
        return;
    }

    const char* const stderrWriterName = "stderr";

    auto rule = New<NLogging::TRuleConfig>();
    rule->Writers.push_back(stderrWriterName);

    if (logLevelFromEnv) {
        Stroka logLevel = logLevelFromEnv;
        logLevel.to_upper(0, std::min(logLevel.size(), static_cast<size_t>(1)));
        rule->MinLevel = TEnumTraits<NLogging::ELogLevel>::FromString(logLevel);
    } else {
        rule->MinLevel = NLogging::ELogLevel::Fatal;
    }

    VectorStrok logExcludeCategories;
    if (logExcludeCategoriesFromEnv) {
        SplitStroku(&logExcludeCategories, logExcludeCategoriesFromEnv, ",");
    }

    for (const auto& excludeCategory : logExcludeCategories) {
        rule->ExcludeCategories.insert(excludeCategory);
    }

    VectorStrok logIncludeCategories;
    if (logIncludeCategoriesFromEnv) {
        SplitStroku(&logIncludeCategories, logIncludeCategoriesFromEnv, ",");
    }

    if (!logIncludeCategories.empty()) {
        rule->IncludeCategories.Assign(yhash_set<Stroka>());
        for (const auto& includeCategory : logIncludeCategories) {
            rule->IncludeCategories->insert(includeCategory);
        }
    }

    auto config = New<NLogging::TLogConfig>();
    config->Rules.push_back(std::move(rule));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    auto stderrWriter = New<NLogging::TWriterConfig>();
    stderrWriter->Type = NLogging::EWriterType::Stderr;

    config->WriterConfigs.insert(std::make_pair(stderrWriterName, std::move(stderrWriter)));

    NLogging::TLogManager::Get()->Configure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

Stroka GenerateRandomFileName(const char* prefix)
{
    return Format("%s-%016" PRIx64 "-%016" PRIx64,
        prefix,
        MicroSeconds(),
        RandomNumber<ui64>());
}

TShadowingLifecycle::TShadowingLifecycle() = default;
TShadowingLifecycle::~TShadowingLifecycle() = default;

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
