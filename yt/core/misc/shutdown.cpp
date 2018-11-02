#include "shutdown.h"

#include <yt/core/misc/assert.h>

#include <algorithm>
#include <vector>
#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static std::vector<std::pair<double, void(*)()>>* ShutdownCallbacks()
{
    static std::vector<std::pair<double, void(*)()>> shutdownCallbacks;
    return &shutdownCallbacks;
}

static std::atomic<bool> ShutdownStarted = false;

void RegisterShutdownCallback(double priority, void(*callback)())
{
    auto item = std::make_pair(priority, callback);
    auto& list = *ShutdownCallbacks();

    YCHECK(std::find(list.begin(), list.end(), item) == list.end());
    list.push_back(item);
}

void Shutdown()
{
    ShutdownStarted = true;

    auto& list = *ShutdownCallbacks();
    std::sort(list.begin(), list.end());

    for (auto it = list.rbegin(); it != list.rend(); ++it) {
        it->second();
    }
}

bool IsShutdownStarted()
{
    return ShutdownStarted;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
