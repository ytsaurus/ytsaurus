#include <library/cpp/testing/benchmark/bench.h>

#include <yt/yt/library/numeric/binary_search.h>

#include <cmath>

using namespace NYT;

static constexpr long double Pi = 3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863;

Y_CPU_BENCHMARK(BinarySearch_InexpensivePredicate, iface)
{
    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        double res = FloatingPointLowerBound(
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            [] (double x) { return x >= Pi; });
        Y_DO_NOT_OPTIMIZE_AWAY(res);
    }
}

Y_CPU_BENCHMARK(BinarySearch_ExpensivePredicate, iface)
{
    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        double res = FloatingPointLowerBound(
            /* lo */ std::numeric_limits<double>::min(),
            /* hi */ std::numeric_limits<double>::max(),
            [] (double x) { return std::log(static_cast<long double>(x)) >= Pi; });
        Y_DO_NOT_OPTIMIZE_AWAY(res);
    }
}
