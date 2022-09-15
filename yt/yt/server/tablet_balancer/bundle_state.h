#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTableProfilingCounters
{
    NProfiling::TCounter InMemoryMoves;
    NProfiling::TCounter ExtMemoryMoves;
    NProfiling::TCounter TabletMerges;
    NProfiling::TCounter TabletSplits;
    NProfiling::TCounter NonTrivialReshards;
};

////////////////////////////////////////////////////////////////////////////////

class TBundleState
    : public TRefCounted
{
public:
    using TTabletMap = THashMap<TTabletId, TTabletPtr>;
    using TTableProfilingCounterMap = THashMap<TTableId, TTableProfilingCounters>;

    DEFINE_BYREF_RO_PROPERTY(TTabletMap, Tablets);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::ETabletCellHealth, Health);
    DEFINE_BYVAL_RO_PROPERTY(TTabletCellBundlePtr, Bundle, nullptr);
    DEFINE_BYREF_RW_PROPERTY(TTableProfilingCounterMap, ProfilingCounters);

public:
    TBundleState(
        TString name,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker);

    void UpdateBundleAttributes(const NYTree::IAttributeDictionary* attributes);

    TFuture<void> UpdateState();
    TFuture<void> FetchStatistics();

private:
    struct TTabletCellInfo
    {
        TTabletCellPtr TabletCell;
        std::vector<TTabletId> TabletIds;
    };

    struct TTableSettings
    {
        TTableTabletBalancerConfigPtr Config;
        EInMemoryMode InMemoryMode;
        bool Dynamic;
    };

    struct TTableStatisticsResponse
    {
        struct TTabletResponse
        {
            i64 Index;
            TTabletId TabletId;

            ETabletState State;
            TTabletStatistics Statistics;
            std::optional<TTabletCellId> CellId;
        };

        std::vector<TTabletResponse> Tablets;
        i64 CompressedDataSize;
        i64 UncompressedDataSize;
    };

    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;

    std::vector<TTabletCellId> CellIds_;

    friend void Deserialize(TTableStatisticsResponse::TTabletResponse& value, const NYTree::INodePtr& node);

    void DoUpdateState();

    THashMap<TTabletCellId, TTabletCellInfo> FetchTabletCells() const;
    THashMap<TTabletId, TTableId> FetchTabletTableIds(
        const THashSet<TTabletId>& tabletIds) const;
    THashMap<TTableId, TTablePtr> FetchBasicTableAttributes(
        const THashSet<TTableId>& tableIds) const;

    void DoFetchStatistics();

    THashMap<TTableId, TTableSettings> FetchActualTableSettings() const;
    THashMap<TTableId, TTableStatisticsResponse> FetchTableStatistics(
        const THashSet<TTableId>& tableIds) const;

    bool IsTableBalancingAllowed(const TTableSettings& table) const;

    void InitializeProfilingCounters(const TTablePtr& table);
};

DEFINE_REFCOUNTED_TYPE(TBundleState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
