#pragma once

#include "public.h"
#include "source_location.h"

#include <yt/core/actions/callback.h>

#include <yt/core/concurrency/fork_aware_spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedTrackerStatistics
{
    struct TStatistics
    {
        size_t ObjectsAlive = 0;
        size_t ObjectsAllocated = 0;
        size_t BytesAlive = 0;
        size_t BytesAllocated = 0;

        TStatistics& operator+= (const TStatistics& rhs);
    };

    struct TNamedSlotStatistics
        : public TStatistics
    {
        TString FullName;
    };

    std::vector<TNamedSlotStatistics> NamedStatistics;
    TStatistics TotalStatistics;
};

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

    TRefCountedTypeCookie GetCookie(
        TRefCountedTypeKey typeKey,
        size_t instanceSize,
        const TSourceLocation& location = TSourceLocation());

    void AllocateInstance(TRefCountedTypeCookie cookie);
    void FreeInstance(TRefCountedTypeCookie cookie);

    void AllocateTagInstance(TRefCountedTypeCookie cookie);
    void FreeTagInstance(TRefCountedTypeCookie cookie);

    void AllocateSpace(TRefCountedTypeCookie cookie, size_t size);
    void FreeSpace(TRefCountedTypeCookie cookie, size_t sie);
    void ReallocateSpace(TRefCountedTypeCookie cookie, size_t sizeFreed, size_t sizeAllocated);

    TString GetDebugInfo(int sortByColumn = -1) const;
    TRefCountedTrackerStatistics GetStatistics() const;

    size_t GetInstancesAllocated(TRefCountedTypeKey typeKey) const;
    size_t GetInstancesAlive(TRefCountedTypeKey typeKey) const;
    size_t GetBytesAllocated(TRefCountedTypeKey typeKey) const;
    size_t GetBytesAlive(TRefCountedTypeKey typeKey) const;

    int GetTrackedThreadCount() const;

private:
    class TStatisticsHolder;
    friend class TStatisticsHolder;
    friend class TRefCountedTrackerInitializer;

    struct TKey
    {
        TRefCountedTypeKey TypeKey;
        TSourceLocation Location;

        bool operator < (const TKey& other) const;
        bool operator == (const TKey& other) const;
    };

    class TAnonymousSlot
    {
    public:
        void AllocateInstance();
        void FreeInstance();

        void AllocateTagInstance();
        void FreeTagInstance();

        void AllocateSpace(size_t size);
        void FreeSpace(size_t size);
        void ReallocateSpace(size_t sizeFreed, size_t sizeAllocated);

        TAnonymousSlot& operator += (const TAnonymousSlot& other);

    protected:
        size_t InstancesAllocated_ = 0;
        size_t InstancesFreed_ = 0;
        size_t TagInstancesAllocated_ = 0;
        size_t TagInstancesFreed_ = 0;
        size_t SpaceSizeAllocated_ = 0;
        size_t SpaceSizeFreed_ = 0;

    };

    using TAnonymousStatistics = std::vector<TAnonymousSlot>;

    class TNamedSlot
        : public TAnonymousSlot
    {
    public:
        TNamedSlot(const TKey& key, size_t instanceSize);

        TRefCountedTypeKey GetTypeKey() const;
        const TSourceLocation& GetLocation() const;

        TString GetTypeName() const;
        TString GetFullName() const;

        size_t GetInstancesAllocated() const;
        size_t GetInstancesAlive() const;

        size_t GetBytesAllocated() const;
        size_t GetBytesAlive() const;

        TRefCountedTrackerStatistics::TNamedSlotStatistics GetStatistics() const;

    private:
        TKey Key_;
        size_t InstanceSize_;

        static size_t ClampNonnegative(size_t allocated, size_t freed);
    };

    using TNamedStatistics = std::vector<TNamedSlot>;

    static PER_THREAD TAnonymousSlot* CurrentThreadStatisticsBegin;
    static PER_THREAD int CurrentThreadStatisticsSize;

    mutable NConcurrency::TForkAwareSpinLock SpinLock_;
    std::map<TKey, TRefCountedTypeCookie> KeyToCookie_;
    std::map<TRefCountedTypeKey, size_t> TypeKeyToInstanceSize_;
    std::vector<TKey> CookieToKey_;
    TAnonymousStatistics GlobalStatistics_;
    THashSet<TStatisticsHolder*> PerThreadHolders_;


    TRefCountedTracker() = default;

    TNamedStatistics GetSnapshot() const;
    static void SortSnapshot(TNamedStatistics* snapshot, int sortByColumn);

    size_t GetInstanceSize(TRefCountedTypeKey typeKey) const;

    TNamedSlot GetSlot(TRefCountedTypeKey typeKey) const;
    TAnonymousSlot* GetPerThreadSlot(TRefCountedTypeCookie cookie);

    void PreparePerThreadSlot(TRefCountedTypeCookie cookie);
    void FlushPerThreadStatistics(TStatisticsHolder* holder);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define REF_COUNTED_TRACKER_INL_H_
#include "ref_counted_tracker-inl.h"
#undef REF_COUNTED_TRACKER_INL_H_
