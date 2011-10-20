#include "../misc/stdafx.h"
#include "foreach.h"
#include "demangle.h"
#include "ref_counted_tracker.h"

#include "../ytree/ytree.h"
#include "../ytree/ephemeral.h"
#include "../ytree/fluent.h"
#include "../ytree/tree_builder.h"

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

void TRefCountedTracker::SortItems(yvector<TItem>& items, int sortByColumn)
{
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
                    return lhs.CreatedObjects > rhs.CreatedObjects;
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
    FOREACH(const auto& item, items) {
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

void TRefCountedTracker::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    auto items = GetItems();
    SortItems(items, -1);
    
    i64 totalAlive = 0;
    i64 totalCreated = 0;

    auto current = NYTree::TFluentYsonBuilder::Create(consumer)
        .BeginMap()
            .Item("statistics").BeginList();

    FOREACH(const auto& item, items) {
        totalAlive += item.AliveObjects;
        totalCreated += item.CreatedObjects;

        current = current
                .Item().BeginMap()
                    .Item("name").Scalar(DemangleCxxName(item.Key->name()))
                    .Item("created").Scalar(static_cast<i64>(item.CreatedObjects))
                    .Item("alive").Scalar(static_cast<i64>(item.AliveObjects))
                .EndMap();
    }

    current
            .EndList()
            .Item("total").BeginMap()
                .Item("created").Scalar(totalCreated)
                .Item("alive").Scalar(totalAlive)
            .EndMap()
        .EndMap();
}

i64 TRefCountedTracker::GetAliveObjects(TKey key)
{
    return Lookup(key)->AliveObjects;
}

i64 TRefCountedTracker::GetCreatedObjects(TKey key)
{
    return Lookup(key)->CreatedObjects;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

