#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/getopt/last_getopt.h>

#include <vector>

namespace NYT {

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "ThrottlerTest");
static auto DonePromise = NewPromise<void>();

enum class ERunMode
{
    Throttle,
    Acquire
};

void RunThrottle(
    NConcurrency::IThroughputThrottlerPtr throttler,
    int i,
    const i64 iterCount,
    const i64 iterSize,
    const TError& error)
{
    YT_VERIFY(error.IsOK());

    while (i < iterCount) {
        if (++i == iterCount) {
            break;
        }
        auto future = throttler->Throttle(iterSize);
        if (!future.IsSet()) {
            future.Subscribe(BIND(&RunThrottle, Passed(std::move(throttler)), i, iterCount, iterSize));
            break;
        }
    }

    if (i == iterCount) {
        DonePromise.Set();
    }
}

void RunAcquire(
    NConcurrency::IThroughputThrottlerPtr throttler,
    i64 iterCount,
    i64 iterSize)
{
    for (auto i = 0; i < iterCount; ) {
        if (!throttler->IsOverdraft()) {
            throttler->Acquire(iterSize);
            ++i;
        }
    }

    DonePromise.Set();
}

void Main(ERunMode mode, double limit, i64 iterCount, i64 iterSize)
{
    auto config = New<NConcurrency::TThroughputThrottlerConfig>();
    config->Limit = limit;
    config->Period = TDuration::MilliSeconds(1000);
    auto throttler = NConcurrency::CreateReconfigurableThroughputThrottler(
        std::move(config),
        Logger());

    // Minus one is because throttler starts out "filled".
    auto timeEstimate = std::max(0.0, double(iterCount) * iterSize / limit - 1);

    Cout << "Running " << iterCount << " calls to "
         << (mode == ERunMode::Throttle ? "Throttle(" : "Acquire(") << iterSize << ")." << Endl;
    Cout << "Limit is set to " << limit << "." << Endl;
    Cout << "The run should take about " << timeEstimate << " s." << Endl;

    NProfiling::TWallTimer timer;

    switch (mode) {
        case ERunMode::Throttle:
            RunThrottle(std::move(throttler), 0, iterCount, iterSize, {});
            break;

        case ERunMode::Acquire:
            RunAcquire(std::move(throttler), iterCount, iterSize);
            break;

        default:
            YT_ABORT();
    }

    DonePromise.ToFuture().Get();

    Cout << "The run took " << timer.GetElapsedTime().SecondsFloat() << " s." << Endl;
}

} // namespace NYT

int main(int argc, const char** argv)
{
    double limit = 0;
    i64 iterCount = 0;
    i64 iterSize = 0;
    auto runThrottle = false;
    auto runAcquire = false;

    NLastGetopt::TOpts opts;
    opts.AddHelpOption();
    opts.AddLongOption("run-throttle-test", "repeatedly call Throttle(), waiting for resulting future unless it is set")
        .StoreTrue(&runThrottle);
    opts.AddLongOption("run-acquire-test", "repeatedly call Acquire(), checking IsOverdraft()")
        .StoreTrue(&runAcquire);
    opts.AddLongOption("limit", "throttler limit")
        .StoreResult(&limit)
        .Required();
    opts.AddLongOption("request-count", "number of iterations")
        .StoreResult(&iterCount)
        .Required();
    opts.AddLongOption("request-size", "the argument passed to every call to Throttle()/Acquire()")
        .StoreResult(&iterSize)
        .Required();


    try {
        NLastGetopt::TOptsParseResult result(&opts, argc, argv);

        if (runThrottle == runAcquire) {
            THROW_ERROR_EXCEPTION("Either \"--run-throttle-test\" or \"--run-acquire-test\" must be specified");
        }

        NYT::ERunMode mode;
        if (runThrottle) {
            mode = NYT::ERunMode::Throttle;
        } else if (runAcquire) {
            mode = NYT::ERunMode::Acquire;
        } else {
            YT_ABORT();
        }

        NYT::Main(mode, limit, iterCount, iterSize);

        return 0;
    } catch (const std::exception& ex) {
        Cerr << ToString(NYT::TError(ex)) << Endl;
        return 1;
    }
}
