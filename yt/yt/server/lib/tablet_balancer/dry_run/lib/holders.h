#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell_bundle.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer::NDryRun {

////////////////////////////////////////////////////////////////////////////////

struct TTabletHolder
    : public NYTree::TYsonStruct
{
    THashMap<TString, i64> Statistics;
    TString PerformanceCounters;

    TTabletId TabletId;
    TTabletCellId CellId;

    // For unittests only, use it instead of ids.
    int TabletIndex;
    int CellIndex;

    TTabletPtr CreateTablet(
        const NTabletBalancer::TTablePtr& table,
        const THashMap<TTabletCellId, NTabletBalancer::TTabletCellPtr>& cells) const;

    REGISTER_YSON_STRUCT(TTabletHolder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHolder)

////////////////////////////////////////////////////////////////////////////////

struct TTableHolder
    : public NYTree::TYsonStruct
{
    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    EInMemoryMode InMemoryMode;
    std::vector<TTabletHolderPtr> Tablets;
    TTableTabletBalancerConfigPtr Config;

    TTableId TableId;

    // For unittests only, use it instead of ids.
    int TableIndex;

    TTablePtr CreateTable(const TTabletCellBundlePtr& bundle) const;

    REGISTER_YSON_STRUCT(TTableHolder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableHolder)

////////////////////////////////////////////////////////////////////////////////

struct TNodeHolder
    : public NYTree::TYsonStruct
{
    TString NodeAddress;
    i64 MemoryUsed;
    std::optional<i64> MemoryLimit;

    TTabletCellBundle::TNodeMemoryStatistics GetStatistics() const;

    REGISTER_YSON_STRUCT(TNodeHolder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeHolder)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellHolder
    : public NYTree::TYsonStruct
{
    i64 MemorySize;
    std::optional<TString> NodeAddress;

    TTabletCellId CellId;

    // For unittests only, use it instead of ids.
    int CellIndex;

    TTabletCellPtr CreateCell() const;

    REGISTER_YSON_STRUCT(TTabletCellHolder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellHolder)

////////////////////////////////////////////////////////////////////////////////

struct TBundleHolder
    : public NYTree::TYsonStruct
{
    TBundleTabletBalancerConfigPtr Config;
    std::vector<TTableHolderPtr> Tables;
    std::vector<TNodeHolderPtr> Nodes;
    std::vector<TTabletCellHolderPtr> Cells;

    TTabletCellBundlePtr CreateBundle() const;

    REGISTER_YSON_STRUCT(TBundleHolder);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleHolder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
