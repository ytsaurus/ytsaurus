#include "stdafx.h"
#include "ref_counted_tracker.h"
#include "demangle.h"

#include <core/ytree/fluent.h>

#include <algorithm>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker::TSlot::TSlot(TRefCountedKey key)
    : Key_(key)
{ }

TRefCountedKey TRefCountedTracker::TSlot::GetKey() const
{
    return Key_;
}

Stroka TRefCountedTracker::TSlot::GetName() const
{
    return DemangleCxxName(Key_->name());
}

TRefCountedTracker::TSlot& TRefCountedTracker::TSlot::operator+=(const TSlot& other)
{
    ObjectsAllocated_ += other.ObjectsAllocated_;
    BytesAllocated_ += other.BytesAllocated_;
    ObjectsFreed_ += other.ObjectsFreed_;
    BytesFreed_ += other.BytesFreed_;
    return *this;
}

size_t TRefCountedTracker::TSlot::GetObjectsAllocated() const
{
    return ObjectsAllocated_;
}

size_t TRefCountedTracker::TSlot::GetObjectsAlive() const
{
    return ObjectsAllocated_ - ObjectsFreed_;
}

size_t TRefCountedTracker::TSlot::GetBytesAllocated() const
{
    return BytesAllocated_;
}

size_t TRefCountedTracker::TSlot::GetBytesAlive() const
{
    return BytesAllocated_ - BytesFreed_;
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker* TRefCountedTracker::Get()
{
    return Singleton<TRefCountedTracker>();
}

TRefCountedTracker::TCookie TRefCountedTracker::GetCookie(TRefCountedKey key)
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    auto it = KeyToCookie_.find(key);
    if (it != KeyToCookie_.end()) {
        return it->second;
    }
    auto cookie = ++LastUsedCookie;
    KeyToCookie_.insert(std::make_pair(key, cookie));
    return cookie;
}

std::vector<TRefCountedTracker::TSlot> TRefCountedTracker::GetSnapshot() const
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    std::vector<TSlot> result;
    for (const auto& pair : KeyToSlot_) {
        result.push_back(pair.second);
    }
    return result;
}

void TRefCountedTracker::SortSnapshot(TStatistics* snapshot, int sortByColumn)
{
    std::function<bool(const TSlot& lhs, const TSlot& rhs)> predicate;
    switch (sortByColumn) {
        case 0:
        default:
            predicate = [] (const TSlot& lhs, const TSlot& rhs) {
                return lhs.GetObjectsAlive() > rhs.GetObjectsAlive();
            };
            break;

        case 1:
            predicate = [] (const TSlot& lhs, const TSlot& rhs) {
                return lhs.GetObjectsAllocated() > rhs.GetObjectsAllocated();
            };
            break;

        case 2:
            predicate = [] (const TSlot& lhs, const TSlot& rhs) {
                return lhs.GetBytesAlive() > rhs.GetBytesAlive();
            };
            break;

        case 3:
            predicate = [] (const TSlot& lhs, const TSlot& rhs) {
                return lhs.GetBytesAllocated() > rhs.GetBytesAllocated();
            };
            break;

        case 4:
            predicate = [] (const TSlot& lhs, const TSlot& rhs) {
                return strcmp(lhs.GetKey()->name(), rhs.GetKey()->name()) < 0;
            };
            break;
    }
    std::sort(snapshot->begin(), snapshot->end(), predicate);
}

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn) const
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
            ~slot.GetName());
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

TYsonProducer TRefCountedTracker::GetMonitoringProducer() const
{
    return BIND([=] (IYsonConsumer* consumer) {
        auto slots = GetSnapshot();
        SortSnapshot(slots, -1);

        size_t totalObjectsAlive = 0;
        size_t totalObjectsAllocated = 0;
        size_t totalBytesAlive = 0;
        size_t totalBytesAllocated = 0;

        for (const auto& slot : slots) {
            totalObjectsAlive += slot.GetObjectsAlive();
            totalObjectsAllocated += slot.GetObjectsAllocated();
            totalBytesAlive += slot.GetBytesAlive();
            totalBytesAllocated += slot.GetBytesAllocated();
        }

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("statistics").DoListFor(slots, [] (TFluentList fluent, const TSlot& slot) {
                    fluent
                        .Item().BeginMap()
                            .Item("name").Value(slot.GetName())
                            .Item("objects_alive").Value(slot.GetObjectsAlive())
                            .Item("objects_allocated").Value(slot.GetObjectsAllocated())
                            .Item("bytes_alive").Value(slot.GetBytesAlive())
                            .Item("bytes_allocated").Value(slot.GetBytesAllocated())
                        .EndMap();
                })
                .Item("total").BeginMap()
                    .Item("objects_alive").Value(totalObjectsAlive)
                    .Item("objects_allocated").Value(totalObjectsAllocated)
                    .Item("bytes_alive").Value(totalBytesAlive)
                    .Item("bytes_allocated").Value(totalBytesAllocated)
                .EndMap()
            .EndMap();
    });
}

i64 TRefCountedTracker::GetObjectsAllocated(TRefCountedKey key)
{
    return GetSlot(key).GetObjectsAllocated();
}

i64 TRefCountedTracker::GetObjectsAlive(TRefCountedKey key)
{
    return GetSlot(key).GetObjectsAlive();
}

i64 TRefCountedTracker::GetAllocatedBytes(TRefCountedKey key)
{
    return GetSlot(key).GetBytesAllocated();
}

i64 TRefCountedTracker::GetAliveBytes(TRefCountedKey key)
{
    return GetSlot(key).GetBytesAlive();
}

TRefCountedTracker::TSlot TRefCountedTracker::GetSlot(TRefCountedKey key)
{
    auto cookie = GetCookie(key);
    TSlot result(key);

    TGuard<TForkAwareSpinLock> guard(SpinLock_);

    if (cookie < GlobalStatistics_.size()) {
        result += GlobalStatistics_[cookie];
    }

    for (const auto* statistics : PerThreadStatistics_) {
        if (cookie < statistics->size()) {
            result += (*statistics)[cookie];
        }
    }

    return result;
}

void TRefCountedTracker::CreateCurrentThreadStatistics()
{
    TGuard<TForkAwareSpinLock> guard(SpinLock_);
    CurrentThreadStatistics = new TStatistics();
    YCHECK(PerThreadStatistics_.insert(CurrentThreadStatistics).second);
}

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn)
{
    fprintf(stderr, "%s", ~TRefCountedTracker::Get()->GetDebugInfo(sortByColumn));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

