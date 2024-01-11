#include <yt/yt/core/misc/signal_registry.h>
#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/misc/tls.h>

#include <util/datetime/base.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

YT_THREAD_LOCAL(int) ThreadIndex;

void CustomCrashSignalHandler()
{
    Cerr << Format("I am %v and I crashed\n", ThreadIndex);
    Sleep(TDuration::Seconds(1));
}

void Main()
{
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CustomCrashSignalHandler);
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    // Uncommenting this line should lead to multiple "I am %v and I crashed"
    // lines visible before program termination.
    // TSignalRegistry::Get()->SetEnableCrashSignalProtection(false);

    auto threadPool = CreateThreadPool(8, "Test");

    auto crashPromise = NewPromise<void>();
    auto crashFuture = crashPromise.ToFuture();

    std::vector<TPromise<void>> readyPromises;
    std::vector<TFuture<void>> readyFutures;

    const int ThreadCount = 8;

    for (int index = 0; index < ThreadCount; ++index) {
        auto& promise = readyPromises.emplace_back(NewPromise<void>());
        readyFutures.emplace_back(promise.ToFuture());
    }

    for (int index = 0; index < ThreadCount; ++index) {
        threadPool->GetInvoker()->Invoke(BIND([&, index] {
            ThreadIndex = index;
            Cerr << Format("Thread %v is ready\n", index);
            readyPromises[index].Set();
            crashFuture.Get();
            YT_ABORT();
        }));
    }

    AllSucceeded(readyFutures).Get();

    Cerr << Format("Crashing!") << Endl;

    crashPromise.Set();

    while (true);
}

////////////////////////////////////////////////////////////////////////////////

}

int main()
{
    NYT::Main();
}

