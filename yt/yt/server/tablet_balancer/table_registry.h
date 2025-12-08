#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTableProfilingCounters
{
    std::string BundleName;
    TString GroupName;
    NProfiling::TCounter InMemoryMoves;
    NProfiling::TCounter OrdinaryMoves;
    NProfiling::TCounter TabletMerges;
    NProfiling::TCounter TabletSplits;
    NProfiling::TCounter NonTrivialReshards;
    NProfiling::TCounter ParameterizedMoves;
    NProfiling::TCounter ReplicaMoves;
    NProfiling::TCounter ParameterizedReshardMerges;
    NProfiling::TCounter ParameterizedReshardSplits;
    NProfiling::TCounter ReplicaMerges;
    NProfiling::TCounter ReplicaSplits;
    NProfiling::TCounter ReplicaNonTrivialReshards;
};

// Per bundle enity.
class TTableRegistry final
{
public:
    using TTableMap = THashMap<TTableId, TTablePtr>;

    DEFINE_BYREF_RO_PROPERTY(TTableMap, Tables);

public:
    void AddTable(const TTablePtr& table);
    void RemoveTable(const TTableId& tableId);

    TTableProfilingCounters& GetProfilingCounters(const TTable* table, const TString& groupName);

private:
    THashSet<TTableId> TablesWithAlienTable_;

    // Never remove profiling counters, even for removed tables.
    // It allowes us to use one instance of table registry in different bundle snapshots.
    THashMap<TTableId, TTableProfilingCounters> ProfilingCounters_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProfilingCountersLock_);

    void UnlinkTableFromOldBundle(const TTablePtr& table);
    void UnlinkTabletFromCell(const TTabletPtr& tablet);
    TTableProfilingCounters InitializeProfilingCounters(const TTable* table, const TString& groupName) const;
};

DEFINE_REFCOUNTED_TYPE(TTableRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
