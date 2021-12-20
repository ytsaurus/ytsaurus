#include "undumpable.h"

#if defined(_linux_)
#include <sys/mman.h>
#endif

#include <util/generic/hash.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/new.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>
#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TUndumpableMark
{
    // TUndumpableMark-s are never freed. All objects are linked through NextMark.
    TUndumpableMark* NextMark = nullptr;

    TUndumpableMark* NextFree = nullptr;

    void* Ptr = nullptr;
    size_t Size = 0;
};

struct TUndumpableSet
{
public:
    constexpr TUndumpableSet() = default;

    TUndumpableMark* MarkUndumpable(void* ptr, size_t size)
    {
        UndumpableBytes_.fetch_add(size, std::memory_order_relaxed);

        auto guard = Guard(Lock_);
        auto mark = GetFree();
        mark->Ptr = ptr;
        mark->Size = size;
        return mark;
    }

    void UnmarkUndumpable(TUndumpableMark* mark)
    {
        UndumpableBytes_.fetch_sub(mark->Size, std::memory_order_relaxed);

        mark->Size = 0;
        mark->Ptr = nullptr;

        auto guard = Guard(Lock_);
        Free(mark);
    }

    void MarkUndumpableOOB(void* ptr, size_t size)
    {
        auto mark = MarkUndumpable(ptr, size);

        auto guard = Guard(TableLock_);
        if (!MarkTable_) {
            MarkTable_.emplace();
        }
        YT_VERIFY(MarkTable_->emplace(ptr, mark).second);
    }

    void UnmarkUndumpableOOB(void* ptr)
    {
        auto guard = Guard(TableLock_);
        if (!MarkTable_) {
            MarkTable_.emplace();
        }

        auto it = MarkTable_->find(ptr);
        YT_VERIFY(it != MarkTable_->end());
        auto mark = it->second;
        MarkTable_->erase(it);
        guard.Release();

        UnmarkUndumpable(mark);
    }

    size_t GetUndumpableBytesCount() const
    {
        return UndumpableBytes_.load();
    }

    size_t GetUndumpableMemoryFootprint() const
    {
        return Footprint_.load();
    }

    void CutUndumpable()
    {
#if defined(_linux_)
        auto head = All_;
        while (head) {
            int ret = madvise(head->Ptr, head->Size, MADV_DONTDUMP);
            // We intentionally ignore any errors here. Undumpable memory is best-effort.
            Y_UNUSED(ret);

            head = head->NextMark;
        }
#endif
    }

private:
    std::atomic<i64> UndumpableBytes_ = 0;
    std::atomic<i64> Footprint_ = 0;

    NThreading::TSpinLock Lock_;
    TUndumpableMark* All_ = nullptr;
    TUndumpableMark* Free_ = nullptr;

    NThreading::TSpinLock TableLock_;
    std::optional<THashMap<void*, TUndumpableMark*>> MarkTable_;

    TUndumpableMark* GetFree()
    {
        if (Free_) {
            auto mark = Free_;
            Free_ = mark->NextFree;
            return mark;
        }

        auto mark = new TUndumpableMark{};
        Footprint_.fetch_add(sizeof(*mark), std::memory_order_relaxed);

        mark->NextMark = All_;
        All_ = mark;
        return mark;
    }

    void Free(TUndumpableMark* mark)
    {
        mark->NextFree = Free_;
        Free_ = mark;
    }
};

static constinit TUndumpableSet UndumpableSet;

TUndumpableMark* MarkUndumpable(void* ptr, size_t size)
{
    return UndumpableSet.MarkUndumpable(ptr, size);
}

void UnmarkUndumpable(TUndumpableMark* mark)
{
    UndumpableSet.UnmarkUndumpable(mark);
}

void MarkUndumpableOOB(void* ptr, size_t size)
{
    UndumpableSet.MarkUndumpableOOB(ptr, size);
}

void UnmarkUndumpableOOB(void* ptr)
{
    UndumpableSet.UnmarkUndumpableOOB(ptr);
}

size_t GetUndumpableBytesCount()
{
    return UndumpableSet.GetUndumpableBytesCount();
}

size_t GetUndumpableMemoryFootprint()
{
    return UndumpableSet.GetUndumpableMemoryFootprint();
}

void CutUndumpableFromCoredump()
{
    return UndumpableSet.CutUndumpable();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TUndumpableSensors)

struct TUndumpableSensors
    : public TRefCounted
{
    TUndumpableSensors()
    {
        NProfiling::TProfiler profiler{"/memory"};

        profiler.AddFuncGauge("/undumpable_bytes", MakeStrong(this), [] {
            return GetUndumpableBytesCount();
        });

        profiler.AddFuncGauge("/undumpable_footprint", MakeStrong(this), [] {
            return GetUndumpableMemoryFootprint();
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TUndumpableSensors)

static TUndumpableSensorsPtr dummy = New<TUndumpableSensors>();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
