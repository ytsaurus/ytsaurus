#pragma once

#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTableBase
    : public TRefCounted
{
    const NYPath::TYPath Path;
    const TTableId Id;
    const NObjectClient::TCellTag ExternalCellTag;
    std::vector<TTabletPtr> Tablets;
    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;

    TTableBase(
        NYPath::TYPath path,
        TTableId tableId,
        NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TTableBase)

struct TTable
    : public TTableBase
{
    const bool Sorted;
    TTabletCellBundle* const Bundle;

    bool Dynamic = false;

    i64 CompressedDataSize = 0;
    i64 UncompressedDataSize = 0;

    EInMemoryMode InMemoryMode = EInMemoryMode::None;
    TTableTabletBalancerConfigPtr TableConfig;

    THashMap<TClusterName, std::vector<TAlienTablePtr>> AlienTables;

    TTable(
        bool sorted,
        NYPath::TYPath path,
        NObjectClient::TCellTag cellTag,
        TTableId tableId,
        TTabletCellBundle* bundle);

    std::optional<TGroupName> GetBalancingGroup() const;

    bool IsLegacyMoveBalancingEnabled() const;

    bool IsParameterizedMoveBalancingEnabled() const;
    bool IsParameterizedReshardBalancingEnabled(bool enableParameterizedByDefault) const;

    THashMap<TClusterName, std::vector<NYPath::TYPath>> GetReplicaBalancingMinorTables(
        const std::string& selfClusterName) const;
};

DEFINE_REFCOUNTED_TYPE(TTable)

struct TAlienTable
    : public TTableBase
{
    TAlienTable(
        NYPath::TYPath path,
        TTableId tableId,
        NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TAlienTable)

////////////////////////////////////////////////////////////////////////////////

std::optional<TGroupName> GetBalancingGroup(
    EInMemoryMode inMemoryMode,
    const TTableTabletBalancerConfigPtr& tableConfig,
    const TBundleTabletBalancerConfigPtr& bundleConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
