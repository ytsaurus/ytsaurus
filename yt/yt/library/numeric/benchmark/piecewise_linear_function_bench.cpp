#include <library/cpp/testing/benchmark/bench.h>

#include <yt/yt/library/numeric/piecewise_linear_function.h>

#include <iostream>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static std::vector<double> CreateInputVector(int n, int nSortedSegments)
{
    const int segSize = n / nSortedSegments;
    // For simplicity, we assume that nSortedSegments is a divider of n.
    Y_VERIFY(segSize * nSortedSegments == n);

    std::vector<double> vec(n);
    for (int i = 0; i < n; i++) {
        vec[i] = i;
    }

    for (int segIdx = 0; segIdx < nSortedSegments; segIdx++) {
        for (int i = 0; i < segSize; i++) {
            vec[segIdx * segSize + i] = nSortedSegments * 10 * (i + 1) + segIdx;
        }
    }

    return vec;
}

static std::vector<double> CreateRandomInputVector(int n)
{
    std::vector<double> vec(n);

    // Seed |rng| with a deterministic value to get deterministic result.
    std::mt19937 rng(42 + n);
    std::uniform_real_distribution<double> dst(0, n);

    for (double& x : vec) {
        x = dst(rng);
    }
    return vec;
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_SortedInput_100000, iface)
{
    const auto vec = CreateInputVector(100'000, 1);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_TwoSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(100'000, 2);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_ThreeSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(99'999, 3);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_FourSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(100'000, 4);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_SevenSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(100'002, 7);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_EightSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(100'000, 8);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_NineSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(99'999, 9);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_HundredSortedSegments_100000, iface)
{
    const auto vec = CreateInputVector(100'000, 100);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_RandomInput_100000, iface)
{
    const auto vec = CreateRandomInputVector(100'000);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_SortedInput_800, iface)
{
    const auto vec = CreateInputVector(800, 1);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_TwoSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(800, 2);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_ThreeSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(801, 3);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_FourSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(800, 4);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_SevenSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(805, 7);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_EightSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(800, 8);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_NineSortedSegments_800, iface)
{
    const auto vec = CreateInputVector(801, 9);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

Y_CPU_BENCHMARK(ClearAndSortCriticalPoints_RandomInput_800, iface)
{
    const auto vec = CreateRandomInputVector(800);
    std::vector<double> vecCopy(vec.size());

    for (auto i = 0; i < static_cast<ssize_t>(iface.Iterations()); ++i) {
        vecCopy = vec;
        ClearAndSortCriticalPoints(&vecCopy, 0, vecCopy.size());
        Y_DO_NOT_OPTIMIZE_AWAY(&vecCopy);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
