#include "ref_counted_tracker.h"
#include "demangle.h"

#include <yt/core/concurrency/thread_affinity.h>

#include <util/system/tls.h>

#include <algorithm>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRefCountedTrackerStatistics::TStatistics& TRefCountedTrackerStatistics::TStatistics::operator+= (
    const TRefCountedTrackerStatistics::TStatistics& rhs)
{
    ObjectsAllocated += rhs.ObjectsAllocated;
    ObjectsFreed += rhs.ObjectsFreed;
    ObjectsAlive += rhs.ObjectsAlive;
    BytesAllocated += rhs.BytesAllocated;
    BytesFreed += rhs.BytesFreed;
    BytesAlive += rhs.BytesAlive;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedTracker::TLocalSlotsHolder
{
    std::vector<TLocalSlot> Slots;

    ~TLocalSlotsHolder()
    {
        TRefCountedTracker::Get()->OnLocalSlotsDestroyed(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TRefCountedTracker::TKey::operator==(const TKey& other) const
{
    return
        TypeKey == other.TypeKey &&
        Location == other.Location;
}

bool TRefCountedTracker::TKey::operator<(const TKey& other) const
{
    if (TypeKey < other.TypeKey) {
        return true;
    }
    if (other.TypeKey < TypeKey) {
        return false;
    }
    if (Location < other.Location) {
        return true;
    }
    if (other.Location < Location) {
        return false;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker::TNamedSlot::TNamedSlot(const TKey& key, size_t objectSize)
    : Key_(key)
    , ObjectSize_(objectSize)
{ }

TRefCountedTypeKey TRefCountedTracker::TNamedSlot::GetTypeKey() const
{
    return Key_.TypeKey;
}

const TSourceLocation& TRefCountedTracker::TNamedSlot::GetLocation() const
{
    return Key_.Location;
}

TString TRefCountedTracker::TNamedSlot::GetTypeName() const
{
    return DemangleCxxName(static_cast<const std::type_info*>(GetTypeKey())->name());
}

TString TRefCountedTracker::TNamedSlot::GetFullName() const
{
    const auto& location = Key_.Location;
    return location.IsValid()
        ? Format("%v at %v:%v", GetTypeName(), location.GetFileName(), location.GetLine())
        : GetTypeName();
}

size_t TRefCountedTracker::TNamedSlot::GetObjectsAllocated() const
{
    return ObjectsAllocated_ + TagObjectsAllocated_;
}

size_t TRefCountedTracker::TNamedSlot::GetObjectsFreed() const
{
    return ObjectsFreed_ + TagObjectsFreed_;
}

size_t TRefCountedTracker::TNamedSlot::GetObjectsAlive() const
{
    return
        ClampNonnegative(ObjectsAllocated_, ObjectsFreed_) +
        ClampNonnegative(TagObjectsAllocated_, TagObjectsFreed_);
}

size_t TRefCountedTracker::TNamedSlot::GetBytesAllocated() const
{
    return
        ObjectsAllocated_ * ObjectSize_ +
        SpaceSizeAllocated_;
}

size_t TRefCountedTracker::TNamedSlot::GetBytesFreed() const
{
    return
        ObjectsFreed_ * ObjectSize_ +
        SpaceSizeFreed_;
}

size_t TRefCountedTracker::TNamedSlot::GetBytesAlive() const
{
    return
        ClampNonnegative(ObjectsAllocated_, ObjectsFreed_) * ObjectSize_ +
        ClampNonnegative(SpaceSizeAllocated_, SpaceSizeFreed_);
}

TRefCountedTrackerStatistics::TNamedSlotStatistics TRefCountedTracker::TNamedSlot::GetStatistics() const
{
    TRefCountedTrackerStatistics::TNamedSlotStatistics result;
    result.FullName = GetFullName();
    result.ObjectsAllocated = GetObjectsAllocated();
    result.ObjectsFreed = GetObjectsFreed();
    result.ObjectsAlive = GetObjectsAlive();
    result.BytesAllocated = GetBytesAllocated();
    result.BytesFreed = GetBytesFreed();
    result.BytesAlive = GetBytesAlive();
    return result;
}

size_t TRefCountedTracker::TNamedSlot::ClampNonnegative(size_t allocated, size_t freed)
{
    return allocated >= freed ? allocated - freed : 0;
}

////////////////////////////////////////////////////////////////////////////////

PER_THREAD TRefCountedTracker::TLocalSlot* TRefCountedTracker::LocalSlotsBegin; // = nullptr
PER_THREAD int TRefCountedTracker::LocalSlotsSize; // = 0

int TRefCountedTracker::GetTrackedThreadCount() const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    return static_cast<int>(LocalSlotHolders_.size());
}

TRefCountedTypeCookie TRefCountedTracker::GetCookie(
    TRefCountedTypeKey typeKey,
    size_t objectSize,
    const TSourceLocation& location)
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    TypeKeyToObjectSize_.emplace(typeKey, objectSize);

    TKey key{typeKey, location};
    auto it = KeyToCookie_.find(key);
    if (it != KeyToCookie_.end()) {
        return it->second;
    }

    auto cookie = CookieToKey_.size();
    KeyToCookie_.emplace(key, TRefCountedTypeCookie(cookie));
    CookieToKey_.push_back(key);

    return cookie;
}

TRefCountedTracker::TNamedStatistics TRefCountedTracker::GetSnapshot() const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    TNamedStatistics result;
    for (const auto& key : CookieToKey_) {
        result.emplace_back(key, GetObjectSize(key.TypeKey));
    }

    auto accumulateResult = [&] (const auto& slots) {
        for (auto index = 0; index < result.size() && index < slots.size(); ++index) {
            result[index] += slots[index];
        }
    };

    accumulateResult(GlobalSlots_);
    for (const auto* holder : LocalSlotHolders_) {
        accumulateResult(holder->Slots);
    }

    return result;
}

void TRefCountedTracker::SortSnapshot(TNamedStatistics* snapshot, int sortByColumn)
{
    std::function<bool(const TNamedSlot& lhs, const TNamedSlot& rhs)> predicate;
    switch (sortByColumn) {
        case 0:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetObjectsAlive() > rhs.GetObjectsAlive();
            };
            break;

        case 1:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetObjectsAllocated() > rhs.GetObjectsAllocated();
            };
            break;

        case 2:
        default:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetBytesAlive() > rhs.GetBytesAlive();
            };
            break;

        case 3:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetBytesAllocated() > rhs.GetBytesAllocated();
            };
            break;

        case 4:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetTypeName() < rhs.GetTypeName();
            };
            break;
    }
    std::sort(snapshot->begin(), snapshot->end(), predicate);
}

TString TRefCountedTracker::GetDebugInfo(int sortByColumn) const
{
    auto snapshot = GetSnapshot();
    SortSnapshot(&snapshot, sortByColumn);

    TStringBuilder builder;

    size_t totalObjectsAlive = 0;
    size_t totalObjectsAllocated = 0;
    size_t totalBytesAlive = 0;
    size_t totalBytesAllocated = 0;

    builder.AppendFormat(
        "%10s %10s %15s %15s %s\n",
        "ObjAlive",
        "ObjAllocated",
        "BytesAlive",
        "BytesAllocated",
        "Name");

    builder.AppendString("-------------------------------------------------------------------------------------------------------------\n");

    for (const auto& slot : snapshot) {
        totalObjectsAlive += slot.GetObjectsAlive();
        totalObjectsAllocated += slot.GetObjectsAllocated();
        totalBytesAlive += slot.GetBytesAlive();
        totalBytesAllocated += slot.GetBytesAllocated();

        builder.AppendFormat(
            "%10" PRISZT " %10" PRISZT " %15" PRISZT " %15" PRISZT " %s\n",
            slot.GetObjectsAlive(),
            slot.GetObjectsAllocated(),
            slot.GetBytesAlive(),
            slot.GetBytesAllocated(),
            slot.GetFullName().data());
    }

    builder.AppendString("-------------------------------------------------------------------------------------------------------------\n");
    builder.AppendFormat(
        "%10" PRISZT " %10" PRISZT " %15" PRISZT " %15" PRISZT " %s\n",
        totalObjectsAlive,
        totalObjectsAllocated,
        totalBytesAlive,
        totalBytesAllocated,
        "Total");

    return builder.Flush();
}

TRefCountedTrackerStatistics TRefCountedTracker::GetStatistics() const
{
    auto slots = GetSnapshot();
    SortSnapshot(&slots, -1);

    TRefCountedTrackerStatistics result;
    result.NamedStatistics.reserve(slots.size());

    for (const auto& slot : slots) {
        auto statistics = slot.GetStatistics();
        result.NamedStatistics.push_back(statistics);
        result.TotalStatistics += statistics;
    }

    return result;
}

size_t TRefCountedTracker::GetObjectsAllocated(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetObjectsAllocated();
}

size_t TRefCountedTracker::GetObjectsAlive(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetObjectsAlive();
}

size_t TRefCountedTracker::GetBytesAllocated(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetBytesAllocated();
}

size_t TRefCountedTracker::GetBytesAlive(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetBytesAlive();
}

size_t TRefCountedTracker::GetObjectSize(TRefCountedTypeKey typeKey) const
{
    auto it = TypeKeyToObjectSize_.find(typeKey);
    return it == TypeKeyToObjectSize_.end() ? 0 : it->second;
}

TRefCountedTracker::TNamedSlot TRefCountedTracker::GetSlot(TRefCountedTypeKey typeKey) const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    TKey key{typeKey, TSourceLocation()};

    TNamedSlot result(key, GetObjectSize(typeKey));
    auto accumulateResult = [&] (const auto& slots, TRefCountedTypeCookie cookie) {
        if (cookie < slots.size()) {
            result += slots[cookie];
        }
    };

    auto it = KeyToCookie_.lower_bound(key);
    while (it != KeyToCookie_.end() && it->first.TypeKey == typeKey) {
        auto cookie = it->second;
        accumulateResult(GlobalSlots_, cookie);
        for (auto* holder : LocalSlotHolders_) {
            accumulateResult(holder->Slots, cookie);
        }
        ++it;
    }

    return result;
}

#define INCREMENT_COUNTER_SLOW(name, delta) \
    if (LocalSlotsSize < 0) { \
        TGuard<TForkAwareSpinLock> guard(SpinLock_); \
        GetGlobalSlot(cookie)->name += delta; \
    } else { \
        GetLocalSlot(cookie)->name += delta; \
    }

void TRefCountedTracker::AllocateInstanceSlow(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER_SLOW(ObjectsAllocated, 1)
}

void TRefCountedTracker::FreeInstanceSlow(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER_SLOW(ObjectsFreed, 1)
}

void TRefCountedTracker::AllocateTagInstanceSlow(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER_SLOW(TagObjectsAllocated, 1)
}

void TRefCountedTracker::FreeTagInstanceSlow(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER_SLOW(TagObjectsFreed, 1)
}

void TRefCountedTracker::AllocateSpaceSlow(TRefCountedTypeCookie cookie, size_t space)
{
    INCREMENT_COUNTER_SLOW(SpaceSizeAllocated, space)
}

void TRefCountedTracker::FreeSpaceSlow(TRefCountedTypeCookie cookie, size_t space)
{
    INCREMENT_COUNTER_SLOW(SpaceSizeFreed, space)
}

#undef INCREMENT_COUNTER_SLOW

TRefCountedTracker::TLocalSlot* TRefCountedTracker::GetLocalSlot(TRefCountedTypeCookie cookie)
{
    Y_STATIC_THREAD(TLocalSlotsHolder) Holder;
    auto& slots = Holder.Get().Slots;
    if (cookie >= static_cast<int>(slots.size())) {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        slots.resize(std::max(static_cast<size_t>(cookie + 1), slots.size() * 2));
    }

    YT_VERIFY(LocalSlotsSize >= 0);
    if (!LocalSlotsBegin) {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        YT_VERIFY(LocalSlotHolders_.insert(Holder.GetPtr()).second);
    }

    LocalSlotsBegin = slots.data();
    LocalSlotsSize = static_cast<int>(slots.size());

    return &slots[cookie];
}

TRefCountedTracker::TGlobalSlot* TRefCountedTracker::GetGlobalSlot(TRefCountedTypeCookie cookie)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);
    if (cookie >= static_cast<int>(GlobalSlots_.size())) {
        GlobalSlots_.resize(std::max(static_cast<size_t>(cookie) + 1, GlobalSlots_.size() * 2));
    }
    return &GlobalSlots_[cookie];
}

void TRefCountedTracker::OnLocalSlotsDestroyed(TLocalSlotsHolder* holder)
{
    LocalSlotsBegin = nullptr;
    LocalSlotsSize = -1;

    const auto& localSlots = holder->Slots;

    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    if (GlobalSlots_.size() < localSlots.size()) {
        GlobalSlots_.resize(std::max(localSlots.size(), GlobalSlots_.size() * 2));
    }

    for (auto index = 0; index < localSlots.size(); ++index) {
        GlobalSlots_[index] += localSlots[index];
    }

    YT_VERIFY(LocalSlotHolders_.erase(holder) == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

