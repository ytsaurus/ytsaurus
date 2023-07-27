#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/atomic_ptr.h>

#include <yt/yt/core/profiling/tscp.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <benchmark/benchmark.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TObject final
{
    static constexpr bool EnableHazard = true;
};

using TObjectPtr = TIntrusivePtr<TObject>;

class TAtomicIntrusivePtrAdapter
{
public:
    void Store(TObjectPtr value)
    {
        Underlying_.Store(std::move(value));
    }

    TObjectPtr Load()
    {
        return Underlying_.Acquire();
    }

private:
    TAtomicIntrusivePtr<TObject> Underlying_;
};

template <class T>
class TShardedAtomicIntrusivePtr
{
public:
    void Store(const TIntrusivePtr<T>& ptr)
    {
        for (auto& shard : Shards_) {
            shard.Ptr = ptr;
        }
    }

    TIntrusivePtr<T> Load() const
    {
        return Shards_[NProfiling::TTscp::Get().ProcessorId].Ptr.Acquire();
    }

private:
    struct alignas(2 * CacheLineSize) TShard
    {
        TAtomicIntrusivePtr<T> Ptr;
    };
    std::array<TShard, NProfiling::TTscp::MaxProcessorId> Shards_;
};

class TAtomicPtrAdapter
{
public:
    void Store(TObjectPtr value)
    {
        Underlying_.Store(std::move(value));
    }

    THazardPtr<TObject> Load()
    {
        return Underlying_.AcquireHazard();
    }

private:
    TAtomicPtr<TObject, /*EnableAcquireHazard*/ true> Underlying_;
};

template <class TPtr>
void AtomicPtr_Read(benchmark::State& state)
{
    static TPtr Ptr;
    Ptr.Store(New<TObject>());
    while (state.KeepRunning()) {
        Ptr.Load();
    }
}

BENCHMARK_TEMPLATE(AtomicPtr_Read, TAtomicObject<TObjectPtr>)
    ->ThreadRange(1, 16);
BENCHMARK_TEMPLATE(AtomicPtr_Read, TAtomicIntrusivePtrAdapter)
    ->ThreadRange(1, 16);
BENCHMARK_TEMPLATE(AtomicPtr_Read, TShardedAtomicIntrusivePtr<TObject>)
    ->ThreadRange(1, 16);
BENCHMARK_TEMPLATE(AtomicPtr_Read, TAtomicPtrAdapter)
    ->ThreadRange(1, 16);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
