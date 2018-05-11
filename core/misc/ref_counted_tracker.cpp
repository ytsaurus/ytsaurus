#include "ref_counted_tracker.h"
#include "demangle.h"
#include "shutdown.h"

#include <util/system/tls.h>
#include <util/system/sanitizers.h>

#include <algorithm>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRefCountedTrackerStatistics::TStatistics& TRefCountedTrackerStatistics::TStatistics::operator+= (
    const TRefCountedTrackerStatistics::TStatistics& rhs)
{
    ObjectsAlive += rhs.ObjectsAlive;
    ObjectsAllocated += rhs.ObjectsAllocated;
    BytesAlive += rhs.BytesAlive;
    BytesAllocated += rhs.BytesAllocated;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

class TRefCountedTracker::TStatisticsHolder
{
public:
    bool IsInitialized() const
    {
        return Owner_ != nullptr;
    }

    void Initialize(TRefCountedTracker* owner)
    {
        Owner_ = owner;
        Statistics_.reset(new TAnonymousStatistics());
    }

    TAnonymousStatistics* GetStatistics()
    {
        return Statistics_.get();
    }

    ~TStatisticsHolder()
    {
        Owner_->FlushPerThreadStatistics(this);
        if (IsShutdownStarted()) {
            // During shutdown, the order of static objects destruction is undefined
            // so use-after-free is possible. That's why we allow the leak here.
            // Despite the destruction of TStatisticsHolder object a pointer to
            // statistics is beforehand saved in TRefCountedTracker.
            NSan::MarkAsIntentionallyLeaked(Statistics_.release());
        }
    }

private:
    TRefCountedTracker* Owner_ = nullptr;
    std::unique_ptr<TAnonymousStatistics> Statistics_;
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

TRefCountedTracker::TAnonymousSlot& TRefCountedTracker::TAnonymousSlot::operator+=(const TAnonymousSlot& other)
{
    InstancesAllocated_ += other.InstancesAllocated_;
    InstancesFreed_ += other.InstancesFreed_;
    TagInstancesAllocated_ += other.TagInstancesAllocated_;
    TagInstancesFreed_ += other.TagInstancesFreed_;
    SpaceSizeAllocated_ += other.SpaceSizeAllocated_;
    SpaceSizeFreed_ += other.SpaceSizeFreed_;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker::TNamedSlot::TNamedSlot(const TKey& key, size_t instanceSize)
    : Key_(key)
    , InstanceSize_(instanceSize)
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

size_t TRefCountedTracker::TNamedSlot::GetInstancesAllocated() const
{
    return InstancesAllocated_ + TagInstancesAllocated_;
}

size_t TRefCountedTracker::TNamedSlot::GetInstancesAlive() const
{
    return
        ClampNonnegative(InstancesAllocated_, InstancesFreed_) +
        ClampNonnegative(TagInstancesAllocated_, TagInstancesFreed_);
}

size_t TRefCountedTracker::TNamedSlot::GetBytesAllocated() const
{
    return
        InstancesAllocated_ * InstanceSize_ +
        SpaceSizeAllocated_;
}

size_t TRefCountedTracker::TNamedSlot::GetBytesAlive() const
{
    return
        ClampNonnegative(InstancesAllocated_, InstancesFreed_) * InstanceSize_ +
        ClampNonnegative(SpaceSizeAllocated_, SpaceSizeFreed_);
}

TRefCountedTrackerStatistics::TNamedSlotStatistics TRefCountedTracker::TNamedSlot::GetStatistics() const
{
    TRefCountedTrackerStatistics::TNamedSlotStatistics result;
    result.FullName = GetFullName();
    result.ObjectsAlive = GetInstancesAlive();
    result.ObjectsAllocated = GetInstancesAllocated();
    result.BytesAlive = GetBytesAlive();
    result.BytesAllocated = GetBytesAllocated();
    return result;
}

size_t TRefCountedTracker::TNamedSlot::ClampNonnegative(size_t allocated, size_t freed)
{
    return allocated >= freed ? allocated - freed : 0;
}

int TRefCountedTracker::GetTrackedThreadCount() const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    return PerThreadHolders_.size();
}

////////////////////////////////////////////////////////////////////////////////

PER_THREAD TRefCountedTracker::TAnonymousSlot* TRefCountedTracker::CurrentThreadStatisticsBegin; // = nullptr
PER_THREAD int TRefCountedTracker::CurrentThreadStatisticsSize; // = 0

TRefCountedTypeCookie TRefCountedTracker::GetCookie(
    TRefCountedTypeKey typeKey,
    size_t instanceSize,
    const TSourceLocation& location)
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    TypeKeyToInstanceSize_.emplace(typeKey, instanceSize);

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
        result.emplace_back(key, GetInstanceSize(key.TypeKey));
    }

    auto accumulateResult = [&] (const TAnonymousStatistics& statistics) {
        for (auto index = 0; index < result.size() && index < statistics.size(); ++index) {
            result[index] += statistics[index];
        }
    };

    accumulateResult(GlobalStatistics_);
    for (auto* holder : PerThreadHolders_) {
        accumulateResult(*holder->GetStatistics());
    }

    return result;
}

void TRefCountedTracker::SortSnapshot(TNamedStatistics* snapshot, int sortByColumn)
{
    std::function<bool(const TNamedSlot& lhs, const TNamedSlot& rhs)> predicate;
    switch (sortByColumn) {
        case 0:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetInstancesAlive() > rhs.GetInstancesAlive();
            };
            break;

        case 1:
            predicate = [] (const TNamedSlot& lhs, const TNamedSlot& rhs) {
                return lhs.GetInstancesAllocated() > rhs.GetInstancesAllocated();
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
        totalObjectsAlive += slot.GetInstancesAlive();
        totalObjectsAllocated += slot.GetInstancesAllocated();
        totalBytesAlive += slot.GetBytesAlive();
        totalBytesAllocated += slot.GetBytesAllocated();

        builder.AppendFormat(
            "%10" PRISZT " %10" PRISZT " %15" PRISZT " %15" PRISZT " %s\n",
            slot.GetInstancesAlive(),
            slot.GetInstancesAllocated(),
            slot.GetBytesAlive(),
            slot.GetBytesAllocated(),
            ~slot.GetFullName());
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

size_t TRefCountedTracker::GetInstancesAllocated(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetInstancesAllocated();
}

size_t TRefCountedTracker::GetInstancesAlive(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetInstancesAlive();
}

size_t TRefCountedTracker::GetBytesAllocated(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetBytesAllocated();
}

size_t TRefCountedTracker::GetBytesAlive(TRefCountedTypeKey typeKey) const
{
    return GetSlot(typeKey).GetBytesAlive();
}

size_t TRefCountedTracker::GetInstanceSize(TRefCountedTypeKey typeKey) const
{
    auto it = TypeKeyToInstanceSize_.find(typeKey);
    return it == TypeKeyToInstanceSize_.end() ? 0 : it->second;
}

TRefCountedTracker::TNamedSlot TRefCountedTracker::GetSlot(TRefCountedTypeKey typeKey) const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    TKey key{typeKey, TSourceLocation()};

    TNamedSlot result(key, GetInstanceSize(typeKey));
    auto accumulateResult = [&] (const TAnonymousStatistics& statistics, TRefCountedTypeCookie cookie) {
        if (cookie < statistics.size()) {
            result += statistics[cookie];
        }
    };

    auto it = KeyToCookie_.lower_bound(key);
    while (it != KeyToCookie_.end() && it->first.TypeKey == typeKey) {
        auto cookie = it->second;
        accumulateResult(GlobalStatistics_, cookie);
        for (auto* holder : PerThreadHolders_) {
            accumulateResult(*holder->GetStatistics(), cookie);
        }
        ++it;
    }

    return result;
}

void TRefCountedTracker::PreparePerThreadSlot(TRefCountedTypeCookie cookie)
{
    Y_STATIC_THREAD(TStatisticsHolder) Holder;
    auto* holder = Holder.GetPtr();

    if (!holder->IsInitialized()) {
        holder->Initialize(this);
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        YCHECK(PerThreadHolders_.insert(holder).second);
    }

    auto* statistics = holder->GetStatistics();
    if (statistics->size() <= cookie) {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        statistics->resize(std::max(
            static_cast<size_t>(cookie + 1),
            statistics->size() * 2));
    }

    CurrentThreadStatisticsBegin = statistics->data();
    CurrentThreadStatisticsSize = statistics->size();
}

void TRefCountedTracker::FlushPerThreadStatistics(TStatisticsHolder* holder)
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    const auto& perThreadStatistics = *holder->GetStatistics();
    if (GlobalStatistics_.size() < perThreadStatistics.size()) {
        GlobalStatistics_.resize(std::max(
            perThreadStatistics.size(),
            GlobalStatistics_.size() * 2));
    }
    for (auto index = 0; index < perThreadStatistics.size(); ++index) {
        GlobalStatistics_[index] += perThreadStatistics[index];
    }
    YCHECK(PerThreadHolders_.erase(holder) == 1);
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker* RefCountedTrackerInstance;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

