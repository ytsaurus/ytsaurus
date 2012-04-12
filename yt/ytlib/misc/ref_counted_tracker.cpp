#include "stdafx.h"
#include "ref_counted_tracker.h"
#include "foreach.h"
#include "demangle.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/tree_builder.h>

#include <algorithm>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRefCountedTracker* TRefCountedTracker::Get()
{
    return Singleton<TRefCountedTracker>();
}

TRefCountedTracker::TCookie TRefCountedTracker::GetCookie(TKey key)
{
    TGuard<TSpinLock> guard(SpinLock);

    auto it = Statistics.find(key);
    if (it != Statistics.end()) {
        return &it->second;
    }

    return &Statistics.insert(MakePair(key, TItem(key))).first->second;
}

yvector<TRefCountedTracker::TItem> TRefCountedTracker::GetItems()
{
    TGuard<TSpinLock> guard(SpinLock);
    yvector<TItem> result;
    FOREACH (auto pair, Statistics) {
        result.push_back(pair.second);
    }
    return result;
}

void TRefCountedTracker::SortItems(yvector<TItem>& items, int sortByColumn)
{
    switch (sortByColumn) {
        case 3:
            std::sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return TCharTraits<char>::Compare(lhs.Key->name(), rhs.Key->name()) < 0;
                });
            break;

        case 2:
            std::sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return lhs.CreatedObjects > rhs.CreatedObjects;
                });
            break;

        case 1:
        default:
            std::sort(
                items.begin(),
                items.end(),
                [] (const TItem& lhs, const TItem& rhs) {
                    return lhs.AliveObjects > rhs.AliveObjects;
                });
            break;
    }
}

Stroka TRefCountedTracker::GetDebugInfo(int sortByColumn)
{
    auto items = GetItems();
    SortItems(items, sortByColumn);

    TStringStream stream;
    i64 totalAlive = 0;
    i64 totalCreated = 0;

    stream << "Reference-Counted Object Statistics\n";
    stream << "================================================================================\n";
    stream << Sprintf("%10s %10s %s", "Alive", "Created", "Name") << "\n";
    stream << "--------------------------------------------------------------------------------\n";
    FOREACH (const auto& item, items) {
        totalAlive += item.AliveObjects;
        totalCreated += item.CreatedObjects;

        stream
            << Sprintf("%10" PRId64 " %10" PRId64 " %s",
                (i64) item.AliveObjects,
                (i64) item.CreatedObjects,
                ~DemangleCxxName(item.Key->name()))
            << "\n";
    }
    stream << "--------------------------------------------------------------------------------\n";
    stream << Sprintf("%10"PRId64" %10"PRId64" %s", totalAlive, totalCreated, "Total") << "\n";
    stream << "================================================================================\n";

    return stream;
}

void TRefCountedTracker::GetMonitoringInfo(IYsonConsumer* consumer)
{
    auto items = GetItems();
    SortItems(items, -1);
    
    i64 totalCreated = 0;
    i64 totalAlive = 0;
    FOREACH (const auto& item, items) {
        totalCreated += item.CreatedObjects;
        totalAlive += item.AliveObjects;
    }

    auto current = BuildYsonFluently(consumer)
        .BeginMap()
            .Item("statistics").DoListFor(items, [] (TFluentList fluent, TItem item) {
                fluent
                    .Item().BeginMap()
                        .Item("name").Scalar(DemangleCxxName(item.Key->name()))
                        .Item("created").Scalar(static_cast<i64>(item.CreatedObjects))
                        .Item("alive").Scalar(static_cast<i64>(item.AliveObjects))
                    .EndMap();
            })
            .Item("total").BeginMap()
                .Item("created").Scalar(totalCreated)
                .Item("alive").Scalar(totalAlive)
            .EndMap()
        .EndMap();
}

i64 TRefCountedTracker::GetAliveObjects(TKey key)
{
    return GetCookie(key)->AliveObjects;
}

i64 TRefCountedTracker::GetCreatedObjects(TKey key)
{
    return GetCookie(key)->CreatedObjects;
}

////////////////////////////////////////////////////////////////////////////////

void DumpRefCountedTracker(int sortByColumn)
{
    Cerr << TRefCountedTracker::Get()->GetDebugInfo(sortByColumn) << Endl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

