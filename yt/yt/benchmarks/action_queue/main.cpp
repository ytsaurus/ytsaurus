#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/new.h>

#include <util/string/cast.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;

int main(int argc, char** argv)
{
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " [actions_per_iteration] [iteration_count]" << Endl;
        return -1;
    }

    const i64 actions = FromString<i64>(argv[1]);
    const int iterations = FromString<int>(argv[2]);

    auto queue = New<TActionQueue>();
    for (int index = 0; index < iterations; ++index) {
        Cout << "Mambo number " << index << Endl;

        i64 count = actions;
        auto promise = NewPromise<void>();

        TWallTimer timer;
        for (i64 i = 0; i < actions; ++i) {
            auto callback = BIND([&] () {
                Yield();
                if (--count == 0) {
                    promise.Set();
                }
            });
            queue->GetInvoker()->Invoke(callback);
        }

        Cout << "Enqueued" << Endl;

        promise.Get();

        auto ms = timer.GetElapsedTime().MilliSeconds();

        Cout << "Total time (ms): " << ms << Endl;
        Cout << "Time per action (ms): " << double(ms) / actions << Endl;
    }

    queue->Shutdown();

    return 0;
}
