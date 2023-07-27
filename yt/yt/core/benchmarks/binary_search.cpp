#include <yt/yt/core/misc/algorithm_helpers.h>
#include <library/cpp/yt/assert/assert.h>

#include <array>

#include <benchmark/benchmark.h>

namespace NYT {
namespace {

constexpr size_t ArraySize = 2000;

struct TBinarySearchBranch
{
    template <class TIter, class TPred>
    static TIter Do(TIter begin, TIter end, TPred pred)
    {
        while (begin != end) {
            TIter middle = begin + (end - begin) / 2;
            if (pred(middle)) {
                begin = ++middle;
            } else {
                end = middle;
            }
        }

        return begin;
    }
};

struct TBinarySearchNoBranch
{
    template <class TIter, class TPred>
    static TIter Do(TIter begin, TIter end, TPred pred)
    {
        // Force unsigned count. See https://godbolt.org/.
        size_t count = end - begin;
        while (count != 0) {
            auto half = count / 2;
            auto middle = begin + half;

            if (pred(middle)) {
                begin = ++middle;
                count -= half + 1;
            } else {
                count = half;
            }
        }
        return begin;
    }
};

struct TBinarySearchNoBranch2
{
    template <class TIter, class TPred>
    static TIter Do(TIter begin, TIter end, TPred pred)
    {
        size_t count = end - begin;
        while (count > 1) {
            auto half = count / 2;
            auto middle = begin + half;

            if (pred(middle)) {
                begin = middle;
            }

            count -= half;
        }

        if (count > 0 && pred(begin)) {
            ++begin;
        }

        return begin;
    }
};

struct TExponentialSearch
{
    template <class TIter, class TPred>
    static TIter Do(TIter begin, TIter end, TPred pred)
    {
        return ExponentialSearch(begin, end, pred);
    }
};

struct TExponentialSearch2
{
    template <class TIter, class TPred>
    static TIter Do(TIter begin, TIter end, TPred pred)
    {
        size_t step = 0;
        while (step < static_cast<size_t>(end - begin)) {
            if (pred(begin + step)) {
                begin += step + 1;
                // Step is always 2^n - 1.
                step += step + 1;
            } else {
                while (step > 0) {
                    // Step is always odd.
                    step /= 2;
                    auto middle = begin + step;
                    if (pred(middle)) {
                        // Do not modify middle under condition to forge compiler generate cmov.
                        begin = ++middle;
                    }
                }
                return begin;
            }
        }

        return BinarySearch(begin, end, pred);
    }
};

template <class Search>
void BenchmarkSearch(benchmark::State& state)
{
    std::vector<int> data(ArraySize);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = i;
    }

    int x = 0;
    while (state.KeepRunning()) {
        auto it = Search::Do(data.begin(), data.end(), [=] (auto* it) {
            return *it < x;
        });

        benchmark::DoNotOptimize(it);

        YT_VERIFY(*it == x);

        x += 37;
        if (x >= static_cast<int>(ArraySize / 2)) {
            x -= ArraySize / 2;
        }
    }
}

BENCHMARK_TEMPLATE(BenchmarkSearch, TBinarySearchBranch);
BENCHMARK_TEMPLATE(BenchmarkSearch, TBinarySearchNoBranch);
BENCHMARK_TEMPLATE(BenchmarkSearch, TBinarySearchNoBranch2);

BENCHMARK_TEMPLATE(BenchmarkSearch, TExponentialSearch);
BENCHMARK_TEMPLATE(BenchmarkSearch, TExponentialSearch2);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

