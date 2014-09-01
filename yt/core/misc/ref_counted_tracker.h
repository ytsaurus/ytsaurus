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
private:
    class TSlot
    {
    public:
        explicit TSlot(TRefCountedKey key);

        TRefCountedKey GetKey() const;
        Stroka GetName() const;

        FORCED_INLINE void Allocate(size_t size)
        {
            ++ObjectsAllocated_;
            ++BytesAllocated_;
        }

        FORCED_INLINE void Free(size_t size)
        {
            ++ObjectsFreed_;
            ++BytesFreed_;
        }

        TSlot& operator += (const TSlot& other);

        i64 GetObjectsAllocated() const;
        i64 GetObjectsAlive() const;
        i64 GetBytesAllocated() const;
        i64 GetBytesAlive() const;

    private:
        TRefCountedKey Key_;
        i64 ObjectsAllocated_ = 0;
        i64 BytesAllocated_ = 0;
        i64 ObjectsFreed_ = 0;
        i64 BytesFreed_ = 0;

    };

    typedef std::vector<TSlot> TStatistics;

public:
    static TRefCountedTracker* Get();

    TRefCountedCookie GetCookie(TRefCountedKey key);

    FORCED_INLINE void Allocate(TRefCountedCookie cookie, size_t size)
    {
        GetPerThreadSlot(cookie)->Allocate(size);
    }

    FORCED_INLINE void Free(TRefCountedCookie cookie, size_t size)
    {
        GetPerThreadSlot(cookie)->Free(size);
    }

    Stroka GetDebugInfo(int sortByColumn = -1) const;
    NYTree::TYsonProducer GetMonitoringProducer() const;

    i64 GetObjectsAllocated(TRefCountedKey key);
    i64 GetObjectsAlive(TRefCountedKey key);
    i64 GetAllocatedBytes(TRefCountedKey key);
    i64 GetAliveBytes(TRefCountedKey key);

private:
    NConcurrency::TForkAwareSpinLock SpinLock_;
    yhash_map<TRefCountedKey, TRefCountedCookie> KeyToCookie_;
    TRefCountedCookie LastUsedCookie = -1;
    TStatistics GlobalStatistics_;
    TLS_STATIC TStatistics* CurrentThreadStatistics = nullptr;
    yhash_set<TStatistics*> PerThreadStatistics_;


    TStatistics GetSnapshot() const;
    static void SortSnapshot(TStatistics* snapshot, int sortByColumn);

    TSlot GetSlot(TRefCountedKey key);

    FORCED_INLINE TStatistics* GetPerThreadStatistics()
    {
        if (!CurrentThreadStatistics) {
            CreateCurrentThreadStatistics();
        }
        return CurrentThreadStatistics;
    }

    FORCED_INLINE TSlot* GetPerThreadSlot(TRefCountedCookie cookie)
    {

    }

    void CreateCurrentThreadStatistics();

};

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn = -1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

template <>
struct TSingletonTraits<NYT::TRefCountedTracker>
{
    enum
    {
        Priority = 1024
    };
};
