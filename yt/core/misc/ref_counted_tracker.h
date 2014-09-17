#pragma once

#include "public.h"

#include <core/ytree/yson_producer.h>

#include <core/concurrency/fork_aware_spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Reference tracking relies on uniqueness of std::type_info objects.
// Without uniqueness reference tracking is still functional but lacks precision
// (i. e. some types may have duplicate slots in the accumulated table).
// GCC guarantees std::type_info uniqueness starting from version 3.0
// due to the so-called vague linking.
//
// See also: http://gcc.gnu.org/faq.html#dso
// See also: http://www.codesourcery.com/public/cxx-abi/

class TRefCountedTracker
    : private TNonCopyable
{
public:
    static TRefCountedTracker* Get();
    TRefCountedTypeCookie GetCookie(TRefCountedTypeKey key);

    FORCED_INLINE void Allocate(TRefCountedTypeCookie cookie, size_t size)
    {
        GetPerThreadSlot(cookie)->Allocate(size);
    }

    FORCED_INLINE void Free(TRefCountedTypeCookie cookie, size_t size)
    {
        GetPerThreadSlot(cookie)->Free(size);
    }

    Stroka GetDebugInfo(int sortByColumn = -1) const;
    NYTree::TYsonProducer GetMonitoringProducer() const;

    i64 GetObjectsAllocated(TRefCountedTypeKey key);
    i64 GetObjectsAlive(TRefCountedTypeKey key);
    i64 GetAllocatedBytes(TRefCountedTypeKey key);
    i64 GetAliveBytes(TRefCountedTypeKey key);

    int GetTrackedThreadCount() const;

private:
    class TStatisticsHolder;
    friend class TStatisticsHolder;
    friend class TRefCountedTrackerInitializer;

    class TAnonymousSlot
    {
    public:
        FORCED_INLINE void Allocate(i64 size)
        {
            ++ObjectsAllocated_;
            BytesAllocated_ += size;
        }

        FORCED_INLINE void Free(i64 size)
        {
            ++ObjectsFreed_;
            BytesFreed_ += size;
        }

        TAnonymousSlot& operator += (const TAnonymousSlot& other);

        i64 GetObjectsAllocated() const;
        i64 GetObjectsAlive() const;
        i64 GetBytesAllocated() const;
        i64 GetBytesAlive() const;

    private:
        i64 ObjectsAllocated_ = 0;
        i64 BytesAllocated_ = 0;
        i64 ObjectsFreed_ = 0;
        i64 BytesFreed_ = 0;

    };

    typedef std::vector<TAnonymousSlot> TAnonymousStatistics;

    class TNamedSlot
        : public TAnonymousSlot
    {
    public:
        explicit TNamedSlot(TRefCountedTypeKey key);
        TRefCountedTypeKey GetKey() const;
        Stroka GetName() const;

    private:
        TRefCountedTypeKey Key_;

    };

    typedef std::vector<TNamedSlot> TNamedStatistics;

    static PER_THREAD TAnonymousSlot* CurrentThreadStatisticsBegin;
    static PER_THREAD int CurrentThreadStatisticsSize;

    NConcurrency::TForkAwareSpinLock SpinLock_;
    yhash_map<TRefCountedTypeKey, TRefCountedTypeCookie> KeyToCookie_;
    std::vector<TRefCountedTypeKey> CookieToKey_;
    TAnonymousStatistics GlobalStatistics_;
    yhash_set<TStatisticsHolder*> PerThreadHolders_;


    TRefCountedTracker() = default;

    TNamedStatistics GetSnapshot() const;
    static void SortSnapshot(TNamedStatistics* snapshot, int sortByColumn);

    TNamedSlot GetSlot(TRefCountedTypeKey key);

    FORCED_INLINE TAnonymousSlot* GetPerThreadSlot(TRefCountedTypeCookie cookie)
    {
        if (cookie >= CurrentThreadStatisticsSize) {
            PreparePerThreadSlot(cookie);
        }
        return CurrentThreadStatisticsBegin + cookie;
    }

    void PreparePerThreadSlot(TRefCountedTypeCookie cookie);
    void FlushPerThreadStatistics(TStatisticsHolder* holder);

};

////////////////////////////////////////////////////////////////////////////////

// A nifty counter initializer for TRefCountedTracker.
static class TRefCountedTrackerInitializer
{
public:
    TRefCountedTrackerInitializer();
} RefCountedTrackerInitializer;

// Never destroyed.
extern TRefCountedTracker* RefCountedTrackerInstance;

FORCED_INLINE TRefCountedTracker* TRefCountedTracker::Get()
{
    return RefCountedTrackerInstance;
}

////////////////////////////////////////////////////////////////////////////////

// Typically invoked from GDB console.
void DumpRefCountedTracker(int sortByColumn = -1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
