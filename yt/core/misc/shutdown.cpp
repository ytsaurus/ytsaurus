#include "shutdown.h"

#include <algorithm>

namespace NYT {

std::vector<std::pair<double, void(*)()>>* ShutdownCallbacks()
{
    static std::vector<std::pair<double, void(*)()>> shutdownCallbacks;
    return &shutdownCallbacks;
}


void RegisterShutdownCallback(double priority, void(*callback)())
{
    ShutdownCallbacks()->emplace_back(priority, callback);
}

void Shutdown()
{
    auto& list = *ShutdownCallbacks();
    std::sort(list.begin(), list.end());

    // Remove duplicates in case user messed up and registered same callback multiple times.
    list.erase(std::unique(list.begin(), list.end()), list.end());

    for (auto it = list.rbegin(); it != list.rend(); ++it) {
        it->second();
    }
}

} // namespace NYT
