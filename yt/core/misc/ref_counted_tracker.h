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

    bool Active_;
    NConcurrency::TForkAwareSpinLock SpinLock_;
    yhash_map<TRefCountedTypeKey, TRefCountedTypeCookie> KeyToCookie_;
    std::vector<TRefCountedTypeKey> CookieToKey_;
    TRefCountedTypeCookie LastUsedCookie = -1;
    TAnonymousStatistics GlobalStatistics_;
    yhash_set<TStatisticsHolder*> PerThreadHolders_;


    DECLARE_SINGLETON_FRIEND(TRefCountedTracker);
    TRefCountedTracker();
    ~TRefCountedTracker();

    TNamedStatistics GetSnapshot() const;
    static void SortSnapshot(TNamedStatistics* snapshot, int sortByColumn);

    TNamedSlot GetSlot(TRefCountedTypeKey key);

    TAnonymousSlot* GetPerThreadSlot(TRefCountedTypeCookie cookie);
    void FlushPerThreadStatistics(TStatisticsHolder* holder);

};

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn = -1);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

//template <>
//struct TSingletonTraits<NYT::TRefCountedTracker>
//{
//    enum
//    {
//        Priority = 1024
//    };
//};
