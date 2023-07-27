#include <benchmark/benchmark.h>

#include <library/cpp/yt/assert/assert.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int N = 1024;

void BM_SyncMap(benchmark::State& state)
{
    static TSyncMap<int, int> map;

    for (int i = 0; i < N; i++) {
        map.FindOrInsert(i, [i] { return i; });
    }

    int k = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(k);
        benchmark::DoNotOptimize(map.Find(k));
    }
}

BENCHMARK(BM_SyncMap)->ThreadRange(1, 32);

struct TEntry final
{ };

void BM_HashMap_SingleThread(benchmark::State& state)
{
    THashMap<int, NYT::TIntrusivePtr<TEntry>> map;

    for (int i = 0; i < N; i++) {
        map[i] = New<TEntry>();
    }

    int k = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(k);
        benchmark::DoNotOptimize(map[k]);
    }
}

BENCHMARK(BM_HashMap_SingleThread);

void BM_HashMap_Spinlock(benchmark::State& state)
{
    static YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, lock);
    static THashMap<int, int> map;

    if (state.thread_index() == 0) {
        for (int i = 0; i < N; i++) {
            map[i] = i;
        }
    }

    int k = 0;
    for (auto _ : state) {
        auto guard = Guard(lock);
        benchmark::DoNotOptimize(k);
        benchmark::DoNotOptimize(map[k]);
    }
}

BENCHMARK(BM_HashMap_Spinlock)->ThreadRange(1, 32);

void BM_HashMap_RWSpinlock(benchmark::State& state)
{
    static YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, lock);
    static THashMap<int, int> map;

    if (state.thread_index() == 0) {
        for (int i = 0; i < N; i++) {
            map[i] = i;
        }
    }

    int k = 0;
    for (auto _ : state) {
        auto guard = ReaderGuard(lock);
        benchmark::DoNotOptimize(k);
        benchmark::DoNotOptimize(map[k]);
    }
}

BENCHMARK(BM_HashMap_RWSpinlock)->ThreadRange(1, 32);


struct TObject
{
    static constexpr bool EnableHazard = true;

    std::atomic<bool> Destroyed{false};

    void Access()
    {
        YT_VERIFY(!Destroyed);
    }

    ~TObject()
    {
        Destroyed = true;
    }
};

void BM_HazardPtr_Bug(benchmark::State& state)
{
    static std::atomic<TObject*> ptr{new TObject{}};

    for (auto _ : state) {
        auto hp = THazardPtr<TObject>::Acquire([] {
            return ptr.load();
        });

        hp->Access();
        hp.Reset();

        auto* old = ptr.exchange(new TObject{});

        RetireHazardPointer(old, [] (TObject* ptr) {
            delete ptr;
        });
    }
}

BENCHMARK(BM_HazardPtr_Bug)->Threads(16);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
