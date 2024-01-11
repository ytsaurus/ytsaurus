#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/program/program.h>

using namespace NYT;
using namespace NYT::NConcurrency;

volatile int Dummy;

int Recurse(int depth)
{
    Dummy = depth;
    if (depth >= 0) {
        Recurse(depth + 1);
    }
    return Dummy;
}

int main()
{
    ConfigureCrashHandler();
    auto queue = New<TActionQueue>();
    BIND([] {
        Recurse(0);
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .Get();
}
