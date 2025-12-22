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
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    std::optional<NTabletClient::ETableReplicaMode> ReplicaMode;

    TTableBase(
        NYPath::TYPath path,
        TTableId tableId,
        NObjectClient::TCellTag cellTag,
        NTabletClient::TTableReplicaId upstreamReplicaId = NObjectClient::NullObjectId);
};

DEFINE_REFCOUNTED_TYPE(TTableBase)

struct TTable
    : public TTableBase
{
    const bool Sorted;
    TTabletCellBundle* const Bundle;

    bool Dynamic = false;

    // TODO(alexelexa): Sugar is harmful, especially the syntactic kind.
    // Make them methods if you want it so badly.
    i64 CompressedDataSize = 0;
    i64 UncompressedDataSize = 0;

    EInMemoryMode InMemoryMode = EInMemoryMode::None;
    // TODO(alexelexa): Rename to Config.
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
    bool IsParameterizedReshardBalancingEnabled(
        bool enableParameterizedByDefault,
        bool desiredTabletCountRequired = true) const;

    bool IsReplicaBalancingEnabled() const;
    bool IsReplicaMoveBalancingEnabled() const;
    bool IsReplicaReshardBalancingEnabled() const;

    THashMap<TClusterName, std::vector<NYPath::TYPath>> GetReplicaBalancingMinorTables(
        const std::string& selfClusterName) const;

    TTablePtr Clone(TTabletCellBundle* bundle, bool copyExtendedAttributes) const;
};

DEFINE_REFCOUNTED_TYPE(TTable)

struct TAlienTable
    : public TTableBase
{
    TAlienTable(
        NYPath::TYPath path,
        TTableId tableId,
        NObjectClient::TCellTag cellTag,
        NTabletClient::TTableReplicaId upstreamReplicaId);
};

DEFINE_REFCOUNTED_TYPE(TAlienTable)

////////////////////////////////////////////////////////////////////////////////

std::optional<TGroupName> GetBalancingGroup(
    EInMemoryMode inMemoryMode,
    const TTableTabletBalancerConfigPtr& tableConfig,
    const TBundleTabletBalancerConfigPtr& bundleConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
