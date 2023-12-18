#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/future.h>

#include <vector>

using namespace NYT;
using namespace NYT::NConcurrency;

class TException { };

void Worker(int iterationCount)
{
    for (int i = 0; i < iterationCount; ++i) {
        try {
            throw TException();
        } catch (const TException&) {
        }
    }
}

int main(int argc, char** argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: <enable-cache> <thread-count> <iteration-count>\n");
        return 1;
    }

    bool enableCache = atoi(argv[1]) != 0;
    int threadCount = atoi(argv[2]);
    int iterationCount = atoi(argv[3]);

    if (enableCache) {
        EnablePhdrCache();
    }

    auto threadPool = CreateThreadPool(threadCount, "Workers");
    std::vector<TFuture<void>> asyncResults;
    for (int i = 0; i < threadCount; ++i) {
        asyncResults.push_back(BIND(&Worker, iterationCount)
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }
    AllSucceeded(asyncResults).Get();
    threadPool->Shutdown();

    return 0;
}
