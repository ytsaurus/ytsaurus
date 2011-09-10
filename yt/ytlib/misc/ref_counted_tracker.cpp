#include "foreach.h"
#include "demangle.h"
#include "ref_counted_tracker.h"

#include <util/generic/algorithm.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker::TStatistics TRefCountedTracker::Statistics;
TSpinLock TRefCountedTracker::SpinLock;

TRefCountedTracker::TCookie TRefCountedTracker::Lookup(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);

    auto it = Statistics.find(key);
    if (it != Statistics.end())
        return &it->Second();

    return &Statistics.insert(MakePair(key, TItem(key))).First()->Second();
}

yvector<TRefCountedTracker::TItem> TRefCountedTracker::GetItems()
{
    TGuard<TSpinLock> guard(SpinLock);
    yvector<TItem> result;
    FOREACH(auto pair, Statistics) {
        result.push_back(pair.Second());
    }
    return result;
}

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn)
{
    yvector<TItem> items;
    {
        TGuard<TSpinLock> guard(SpinLock);
        items = GetItems();
    }

    switch (sortByColumn) {
        case 3:
            Sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return TCharTraits<char>::Compare(lhs.Key->name(), rhs.Key->name()) < 0;
                });
            break;

        case 2:
            Sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return lhs.TotalObjects > rhs.TotalObjects;
                });
            break;

        case 1:
        default:
            Sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return lhs.AliveObjects > rhs.AliveObjects;
                });
            break;
    }

    TStringStream stream;
    i64 totalAliveObjects = 0;
    i64 totalTotalObjects = 0;

    stream << "Reference-Counted Object Statistics\n";
    stream << "================================================================================\n";
    stream << Sprintf("%10s %10s %s", "Alive", "Total", "Name") << "\n";
    stream << "--------------------------------------------------------------------------------\n";
    FOREACH(const auto& item, items) {
        totalAliveObjects += item.AliveObjects;
        totalTotalObjects += item.TotalObjects;

        stream
            << Sprintf("%10" PRId64 " %10" PRId64 " %s",
                (i64) item.AliveObjects,
                (i64) item.TotalObjects,
                ~DemangleCxxName(item.Key->name()))
            << "\n";
    }
    stream << "--------------------------------------------------------------------------------\n";
    stream << Sprintf("%10"PRId64" %10"PRId64" %s", totalAliveObjects, totalTotalObjects, "Total") << "\n";
    stream << "================================================================================\n";

    return stream;
}

i64 TRefCountedTracker::GetAliveObjects(TKey key)
{
    return Lookup(key)->AliveObjects;
}

i64 TRefCountedTracker::GetTotalObjects(TKey key)
{
    return Lookup(key)->TotalObjects;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

