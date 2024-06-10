#pragma once

#include <cstdint>
#include <compare>

struct THuMetrics {
    uint64_t AllocatedBytes = 0;
    uint64_t FreedBytes = 0;
    uint64_t AllocationsCount = 0;
    uint64_t FreesCount = 0;

    auto operator<=>(const THuMetrics&) const = default;
};

THuMetrics GetHuMetrics();
