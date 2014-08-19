#include "stdafx.h"
#include "ref_counted_tracker.h"
#include "demangle.h"

#include <core/ytree/fluent.h>

#include <algorithm>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker::TSlot::TSlot(TKey key)
    : Key_(key)
{ }

TRefCountedTracker::TKey TRefCountedTracker::TSlot::GetKey() const
{
    return Key_;
}

Stroka TRefCountedTracker::TSlot::GetName() const
{
    return DemangleCxxName(Key_->name());
}

size_t TRefCountedTracker::TSlot::GetObjectsAllocated() const
{
    return ObjectsAllocated_.Get();
}

size_t TRefCountedTracker::TSlot::GetObjectsAlive() const
{
    return ObjectsAllocated_.Get() - ObjectsFreed_.Get();
}

size_t TRefCountedTracker::TSlot::GetBytesAllocated() const
{
    return BytesAllocated_.Get();
}

size_t TRefCountedTracker::TSlot::GetBytesAlive() const
{
    return BytesAllocated_.Get() - BytesFreed_.Get();
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker* TRefCountedTracker::Get()
{
    return Singleton<TRefCountedTracker>();
}

void* TRefCountedTracker::GetCookie(TKey key)
{
    return GetSlot(key);
}

TRefCountedTracker::TSlot* TRefCountedTracker::GetSlot(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock_);

    auto it = KeyToSlot_.find(key);
    if (it != KeyToSlot_.end()) {
        return &it->second;
    }

    return &KeyToSlot_.insert(std::make_pair(key, TSlot(key))).first->second;
}

std::vector<TRefCountedTracker::TSlot> TRefCountedTracker::GetSnapshot() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    std::vector<TSlot> result;
    for (const auto& pair : KeyToSlot_) {
        result.push_back(pair.second);
    }
    return result;
}

void TRefCountedTracker::SortSnapshot(std::vector<TSlot>& slots, int sortByColumn)
{
    switch (sortByColumn) {
        case 0:
        default:
            std::sort(
                slots.begin(),
                slots.end(),
                [] (const TSlot& lhs, const TSlot& rhs) {
                    return lhs.GetObjectsAlive() > rhs.GetObjectsAlive();
                });
            break;

        case 1:
            std::sort(
                slots.begin(),
                slots.end(),
                [] (const TSlot& lhs, const TSlot& rhs) {
                    return lhs.GetObjectsAllocated() > rhs.GetObjectsAllocated();
                });
            break;

        case 2:
            std::sort(
                slots.begin(),
                slots.end(),
                [] (const TSlot& lhs, const TSlot& rhs) {
                    return lhs.GetBytesAlive() > rhs.GetBytesAlive();
                });
            break;

        case 3:
            std::sort(
                slots.begin(),
                slots.end(),
                [] (const TSlot& lhs, const TSlot& rhs) {
                    return lhs.GetBytesAllocated() > rhs.GetBytesAllocated();
                });
            break;

        case 4:
            std::sort(
                slots.begin(),
                slots.end(),
                [] (const TSlot& lhs, const TSlot& rhs) {
                    return strcmp(lhs.GetKey()->name(), rhs.GetKey()->name()) < 0;
                });
            break;
    }
}

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn) const
{
    auto slots = GetSnapshot();
    SortSnapshot(slots, sortByColumn);

    TStringStream stream;

    size_t totalObjectsAlive = 0;
    size_t totalObjectsAllocated = 0;
    size_t totalBytesAlive = 0;
    size_t totalBytesAllocated = 0;

    stream << Format("%10s %10s %15s %15s %s\n",
        "ObjAlive",
        "ObjAllocated",
        "BytesAlive",
        "BytesAllocated",
        "Name");
    stream << "-------------------------------------------------------------------------------------------------------------\n";

    for (const auto& slot : slots) {
        totalObjectsAlive += slot.GetObjectsAlive();
        totalObjectsAllocated += slot.GetObjectsAllocated();
        totalBytesAlive += slot.GetBytesAlive();
        totalBytesAllocated += slot.GetBytesAllocated();

        stream << Format("%10" PRISZT " %10" PRISZT " %15" PRISZT " %15" PRISZT " %s\n",
            slot.GetObjectsAlive(),
            slot.GetObjectsAllocated(),
            slot.GetBytesAlive(),
            slot.GetBytesAllocated(),
            ~slot.GetName());
    }

    stream << "-------------------------------------------------------------------------------------------------------------\n";
    stream << Format("%10" PRISZT " %10" PRISZT " %15" PRISZT " %15" PRISZT " %s\n",
        totalObjectsAlive,
        totalObjectsAllocated,
        totalBytesAlive,
        totalBytesAllocated,
        "Total");

    return stream;
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

i64 TRefCountedTracker::GetObjectsAllocated(TKey key)
{
    return GetSlot(key)->GetObjectsAllocated();
}

i64 TRefCountedTracker::GetObjectsAlive(TKey key)
{
    return GetSlot(key)->GetObjectsAlive();
}

i64 TRefCountedTracker::GetAllocatedBytes(TKey key)
{
    return GetSlot(key)->GetBytesAllocated();
}

i64 TRefCountedTracker::GetAliveBytes(TKey key)
{
    return GetSlot(key)->GetBytesAlive();
}

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn)
{
    fprintf(stderr, "%s", ~TRefCountedTracker::Get()->GetDebugInfo(sortByColumn));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

