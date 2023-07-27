#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <yt/yt/core/misc/hazard_ptr.h>

#include <util/thread/lfqueue.h>

#include <util/system/mutex.h>
#include <util/system/spinlock.h>

#include <util/stream/str.h>

#include <util/generic/map.h>

#include <benchmark/benchmark.h>

#include <atomic>
#include <deque>
#include <mutex>
#include <thread>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TReporter
{
public:
    void Begin(benchmark::State& /*state*/)
    {
        auto guard = Guard(ReportMutex_);
        ++ConsumerCounter_;
    }

    void End(int n, benchmark::State& state)
    {
        state.SetItemsProcessed(n);
        auto guard = Guard(ReportMutex_);
        ConsumerMap_[state.thread_index()] = n;
        if (--ConsumerCounter_ == 0) {
            int total = 0;
            for (auto& [index, count] : ConsumerMap_) {
                total += count;
            }
            TStringStream ss;
            if (total > 0) {
                for (auto [index, count] : ConsumerMap_) {
                    ss << index << ":" << static_cast<double>(count) * ConsumerMap_.size() / total << " ";
                }
            } else {
                ss << "~0";
            }
            state.SetLabel(ss.Data());
            ConsumerMap_.clear();
        }
    }

private:
    TMutex ReportMutex_;
    TMap<int, int> ConsumerMap_;
    int ConsumerCounter_ = 0;
};

template <typename TQueueImpl>
class TMpmcBenchmark
    : public TQueueImpl
    , public TReporter
    , public benchmark::Fixture
{
public:
    void Execute(benchmark::State& state)
    {
        if ((state.thread_index() % 2) == 0) {
            while (state.KeepRunning()) {
                TQueueImpl::Produce(3);
            }
        } else {
            int N = 0;
            Begin(state);
            while (state.KeepRunning()) {
                int v;
                N += TQueueImpl::Consume(v) ? 1 : 0;
            }
            End(N, state);
        }
    }
};

template <typename TQueueImpl>
class TMpmcTokenBenchmark
    : public TQueueImpl
    , public TReporter
    , public benchmark::Fixture
{
public:
    void Execute(benchmark::State& state)
    {
        if ((state.thread_index() % 2) == 0) {
            while (state.KeepRunning()) {
                TQueueImpl::Produce(3);
            }
        } else {
            int N = 0;
            Begin(state);
            auto token = TQueueImpl::MakeConsumerToken();
            while (state.KeepRunning()) {
                int v;
                N += TQueueImpl::Consume(v, &token) ? 1 : 0;
            }
            End(N, state);
        }
    }
};

template <class T>
struct TMSHPLockFreeQueue
{
    struct TNode
    {
        std::atomic<TNode*> Next = nullptr;
        T Data;

        // True if not dummy and not released.
        std::atomic<bool> Ref = true;
        static constexpr bool EnableHazard = true;
    };

    std::atomic<TNode*> Head;
    std::atomic<TNode*> Tail;

    TMSHPLockFreeQueue()
    {
        auto head = new TNode;
        head->Ref = false;
        Head = head;
        Tail = head;
    }


    void Enqueue(const T& data)
    {
        auto node = new TNode;
        node->Data = data;

        auto tailValue = Tail.load();

        while (true) {
            auto tail = THazardPtr<TNode>::Acquire([&] {
                return Tail.load();
            }, tailValue);

            tailValue = tail.Get();
            auto next = tail->Next.load();

            if (next != nullptr) {
                Tail.compare_exchange_weak(tailValue, next);
                continue;
            }

            if (tailValue->Next.compare_exchange_strong(next, node)) {
                break;
            }

            SpinLockPause();
        }

        Tail.compare_exchange_strong(tailValue, node);
    }

    bool Dequeue(T* data)
    {
        auto headValue = Head.load();

        TNode* next = nullptr;
        while (true) {
            auto head = THazardPtr<TNode>::Acquire([&] {
                return Head.load();
            }, headValue);

            headValue = head.Get();
            next = head->Next.load();

            if (next == nullptr) {
                return false;
            }

            if (Head.compare_exchange_strong(headValue, next)) {
                auto tail = Tail.load(std::memory_order::acquire);

                if (headValue == tail) {
                    Tail.compare_exchange_strong(tail, next);
                }

                break;
            }

            SpinLockPause();
        }

        // Delete head.

        if (!headValue->Ref.load(std::memory_order::acquire) || !headValue->Ref.exchange(false)) {
            // Other thread have already extracted Data and node is dummy.
            RetireHazardPointer(headValue, [] (auto* ptr) {
                delete ptr;
            });
        }
        // Otherwise other thread is not finished next line.
        // But we unlink head from queue and must signal it to thread which moves data.

        *data = std::move(next->Data);

        if (!next->Ref.load(std::memory_order::acquire) || !next->Ref.exchange(false)) {
            // Other thread dequeued next as dummy head.
            RetireHazardPointer(next, [] (auto* ptr) {
                delete ptr;
            });
        }

        return true;
    }

};

class TMSHPLFQueueImpl
{
public:
    void Produce(int val)
    {
        Queue_.Enqueue(val);
    }

    bool Consume(int& v)
    {
        return Queue_.Dequeue(&v);
    }

private:
    TMSHPLockFreeQueue<int> Queue_;
};

class TLFQueueImpl
{
public:
    void Produce(int val)
    {
        Queue_.Enqueue(val);
    }

    bool Consume(int& v)
    {
        return Queue_.Dequeue(&v);
    }

private:
    TLockFreeQueue<int> Queue_;
};

template <typename TLock>
class TDequeLockedImpl
{
public:
    void Produce(int val)
    {
        auto guard = Guard(Lock_);
        Queue_.push_back(val);
    }

    bool Consume(int& val)
    {
        auto guard = Guard(Lock_);
        if (Queue_.empty()) {
            return false;
        }
        val = Queue_.front();
        Queue_.pop_front();
        return true;
    }

private:
    std::deque<int> Queue_;
    TLock Lock_;
};

class TStdMutex
    : public std::mutex
{
public:
    void Acquire()
    {
        lock();
    }

    void Release()
    {
        unlock();
    }
};

class TMoodyCamelQueueImpl
{
public:
    void Produce(int val)
    {
        Queue_.enqueue(val);
    }

    bool Consume(int& v, moodycamel::ConsumerToken* token = nullptr)
    {
        if (token) {
            return Queue_.try_dequeue(*token, v);
        } else {
            return Queue_.try_dequeue(v);
        }
    }

    moodycamel::ConsumerToken MakeConsumerToken()
    {
        return moodycamel::ConsumerToken(Queue_);
    }

private:
    moodycamel::ConcurrentQueue<int> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_MT_BENCHMARK(TBenchmark, Impl) \
    BENCHMARK_DEFINE_F(TBenchmark, Impl)(benchmark::State& state) { Execute(state); } \
    BENCHMARK_REGISTER_F(TBenchmark, Impl)->ThreadRange(1 << 1, 1 << 5)->UseRealTime();

using TMpmcLFQueueBenchmark = TMpmcBenchmark<TLFQueueImpl>;
DECLARE_MT_BENCHMARK(TMpmcLFQueueBenchmark, Mpmc)

using TMpmcDequeSpinLockBenchmark = TMpmcBenchmark<TDequeLockedImpl<TSpinLock>>;
DECLARE_MT_BENCHMARK(TMpmcDequeSpinLockBenchmark, Mpmc)

using TMpmcMSHPLFQueueBenchmark = TMpmcBenchmark<TMSHPLFQueueImpl>;
DECLARE_MT_BENCHMARK(TMpmcMSHPLFQueueBenchmark, Mpmc)

using TMpmcDequeAdaptiveLockBenchmark = TMpmcBenchmark<TDequeLockedImpl<NThreading::TSpinLock>>;
DECLARE_MT_BENCHMARK(TMpmcDequeAdaptiveLockBenchmark, Mpmc)

using TMpmcDequeAdaptiveSpinLockBenchmark = TMpmcBenchmark<TDequeLockedImpl<NThreading::TSpinLock>>;
DECLARE_MT_BENCHMARK(TMpmcDequeAdaptiveSpinLockBenchmark, Mpmc)

using TMpmcDequeStdMutexBenchmark = TMpmcBenchmark<TDequeLockedImpl<TStdMutex>>;
DECLARE_MT_BENCHMARK(TMpmcDequeStdMutexBenchmark, Mpmc)

using TMpmcDequeMutexBenchmark = TMpmcBenchmark<TDequeLockedImpl<TMutex>>;
DECLARE_MT_BENCHMARK(TMpmcDequeMutexBenchmark, Mpmc)

using TMpmcMoodyCamelQueueBenchmark = TMpmcBenchmark<TMoodyCamelQueueImpl>;
DECLARE_MT_BENCHMARK(TMpmcMoodyCamelQueueBenchmark, Mpmc)

using TMpmcMoodyCamelQueueTokenBenchmark = TMpmcTokenBenchmark<TMoodyCamelQueueImpl>;
DECLARE_MT_BENCHMARK(TMpmcMoodyCamelQueueTokenBenchmark, Mpmc)

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
