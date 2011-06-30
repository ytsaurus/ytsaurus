#include "demangle.h"
#include "ref_counted_tracker.h"

#include <util/generic/algorithm.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

unsigned int TRefCountedTracker::GetAliveObjects(const std::type_info& typeInfo) const
{
    TGuard<TSpinLock> guard(&SpinLock);
    TStatisticsMap::const_iterator it = Table.find(&typeInfo);
    return (it == Table.end()) ? 0 : it->Second().AliveObjects;
}

unsigned int TRefCountedTracker::GetTotalObjects(const std::type_info& typeInfo) const
{
    TGuard<TSpinLock> guard(&SpinLock);
    TStatisticsMap::const_iterator it = Table.find(&typeInfo);
    return (it == Table.end()) ? 0 : it->Second().TotalObjects;
}

////////////////////////////////////////////////////////////////////////////////

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn) const
{
    TGuard<TSpinLock> guard(&SpinLock);

    TStatisticsVector values;
    values.reserve(Table.size());
    for (TStatisticsMap::const_iterator it = Table.begin(), jt = Table.end(); it != jt; ++it) {
        values.push_back(MakePair(it->First(), it->Second()));
    }

    switch (sortByColumn) {
        case 3:
            Sort(values.begin(), values.end(), TByName());
            break;
        case 2:
            Sort(values.begin(), values.end(), TByTotalObjects());
            break;
        case 1:
        default:
            Sort(values.begin(), values.end(), TByAliveObjects());
            break;
    }

    TStringStream stream;
    ui64 totalAliveObjects = 0;
    ui64 totalTotalObjects = 0;

    stream << "Reference-Counted Object Statistics\n";
    stream << "================================================================================\n";
    stream << Sprintf("%10s %10s %s", "Alive", "Total", "Name") << "\n";
    stream << "--------------------------------------------------------------------------------\n";
    for (TStatisticsVector::const_iterator it = values.begin(), jt = values.end(); it != jt; ++it)
    {
        totalAliveObjects += it->Second().AliveObjects;
        totalTotalObjects += it->Second().TotalObjects;

        stream << Sprintf("%10d %10d %s",
                          it->Second().AliveObjects,
                          it->Second().TotalObjects,
                          ~DemangleCxxName(it->First()->name())
                          ) << "\n";
    }
    stream << "--------------------------------------------------------------------------------\n";
    stream << Sprintf("%10"PRId64" %10"PRId64" %s", totalAliveObjects, totalTotalObjects, "Total") << "\n";
    stream << "================================================================================\n";

    return stream;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

