#include "demangle.h"
#include "ref_counted_tracker.h"

#include <util/generic/algorithm.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

unsigned int TRefCountedTracker::GetAliveObjects(const std::type_info& typeInfo)
{
    TGuard<TSpinLock> guard(SpinLock);
    return Lookup(&typeInfo)->AliveObjects;
}

unsigned int TRefCountedTracker::GetTotalObjects(const std::type_info& typeInfo)
{
    TGuard<TSpinLock> guard(SpinLock);
    return Lookup(&typeInfo)->TotalObjects;
}

yvector<TRefCountedTracker::TItem> TRefCountedTracker::GetItems()
{
    yvector<TItem> result;
    for (TItem* it = Table.begin(); it != Table.end(); ++it) {
        if (it->Key != NULL) {
            result.push_back(*it);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn)
{
    yvector<TItem> items;
    {
        TGuard<TSpinLock> guard(SpinLock);
        items = GetItems();
    }

    switch (sortByColumn) {
        case 3:
            Sort(items.begin(), items.end(), TByName());
            break;
        case 2:
            Sort(items.begin(), items.end(), TByTotalObjects());
            break;
        case 1:
        default:
            Sort(items.begin(), items.end(), TByAliveObjects());
            break;
    }

    TStringStream stream;
    ui64 totalAliveObjects = 0;
    ui64 totalTotalObjects = 0;

    stream << "Reference-Counted Object Statistics\n";
    stream << "================================================================================\n";
    stream << Sprintf("%10s %10s %s", "Alive", "Total", "Name") << "\n";
    stream << "--------------------------------------------------------------------------------\n";
    for (yvector<TItem>::const_iterator it = items.begin(); it != items.end(); ++it)
    {
        totalAliveObjects += it->AliveObjects;
        totalTotalObjects += it->TotalObjects;

        stream << Sprintf("%10d %10d %s",
                          (i32) it->AliveObjects,
                          (i32) it->TotalObjects,
                          ~DemangleCxxName(it->Key->name())
                          ) << "\n";
    }
    stream << "--------------------------------------------------------------------------------\n";
    stream << Sprintf("%10"PRId64" %10"PRId64" %s", totalAliveObjects, totalTotalObjects, "Total") << "\n";
    stream << "================================================================================\n";

    return stream;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

