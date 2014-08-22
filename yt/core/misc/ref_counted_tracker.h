#pragma once

#include <core/ytree/yson_producer.h>

#include <core/concurrency/counter.h>
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
    typedef const std::type_info* TKey;

private:
    class TSlot
    {
    public:
        explicit TSlot(TKey key);

        TKey GetKey() const;
        Stroka GetName() const;

        FORCED_INLINE void Allocate(size_t size)
        {
            ObjectsAllocated_.Increment(1);
            BytesAllocated_.Increment(size);
        }

        FORCED_INLINE void Free(size_t size)
        {
            ObjectsFreed_.Increment(1);
            BytesFreed_.Increment(size);
        }

        size_t GetObjectsAllocated() const;
        size_t GetObjectsAlive() const;
        size_t GetBytesAllocated() const;
        size_t GetBytesAlive() const;

    private:
        TKey Key_;
        NConcurrency::TCounter ObjectsAllocated_;
        NConcurrency::TCounter BytesAllocated_;
        NConcurrency::TCounter ObjectsFreed_;
        NConcurrency::TCounter BytesFreed_;

    };

public:
    static TRefCountedTracker* Get();

    void* GetCookie(TKey key);

    FORCED_INLINE void Allocate(void* cookie, size_t size)
    {
        static_cast<TSlot*>(cookie)->Allocate(size);
   }

    FORCED_INLINE void Free(void* cookie, size_t size)
    {
        static_cast<TSlot*>(cookie)->Free(size);
    }

    Stroka GetDebugInfo(int sortByColumn = -1) const;
    NYTree::TYsonProducer GetMonitoringProducer() const;

    i64 GetObjectsAllocated(TKey key);
    i64 GetObjectsAlive(TKey key);
    i64 GetAllocatedBytes(TKey key);
    i64 GetAliveBytes(TKey key);

private:
    NConcurrency::TForkAwareSpinLock SpinLock_;
    yhash_map<TKey, TSlot> KeyToSlot_;

    std::vector<TSlot> GetSnapshot() const;
    static void SortSnapshot(std::vector<TSlot>& slots, int sortByColumn);

    TSlot* GetSlot(TKey key);

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
