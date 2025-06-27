
#include "config.h"
#include "table.h"
#include "tablet_cell_bundle.h"

namespace NYT::NTabletBalancer {

using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TTableBase::TTableBase(
    TYPath path,
    TTableId tableId,
    TCellTag cellTag)
    : Path(std::move(path))
    , Id(tableId)
    , ExternalCellTag(cellTag)
{ }

TTable::TTable(
    bool sorted,
    TYPath path,
    TCellTag cellTag,
    TTableId tableId,
    TTabletCellBundle* bundle)
    : TTableBase(std::move(path), tableId, cellTag)
    , Sorted(sorted)
    , Bundle(std::move(bundle))
{ }

std::optional<TGroupName> TTable::GetBalancingGroup() const
{
    return NTabletBalancer::GetBalancingGroup(InMemoryMode, TableConfig, Bundle->Config);
}

bool TTable::IsParameterizedMoveBalancingEnabled() const
{
    if (!TableConfig->EnableAutoTabletMove) {
        return false;
    }

    auto groupName = GetBalancingGroup();

    if (!groupName) {
        return false;
    }

    const auto& bundleConfig = Bundle->Config;
    const auto& groupConfig = GetOrCrash(bundleConfig->Groups, *groupName);
    if (groupConfig->Type != EBalancingType::Parameterized || !groupConfig->EnableMove) {
        return false;
    }

    if (groupName == DefaultGroupName) {
        return TableConfig->EnableParameterized.value_or(bundleConfig->EnableParameterizedByDefault);
    }

    return TableConfig->EnableParameterized.value_or(true);
}

bool TTable::IsParameterizedReshardBalancingEnabled(bool enableParameterizedByDefault) const
{
    if (!TableConfig->EnableAutoReshard) {
        return false;
    }

    auto groupName = GetBalancingGroup();

    if (!groupName) {
        return false;
    }

    const auto& bundleConfig = Bundle->Config;
    const auto& groupConfig = GetOrCrash(bundleConfig->Groups, *groupName);
    if (groupConfig->Type != EBalancingType::Parameterized || !groupConfig->EnableReshard) {
        return false;
    }

    if (!groupConfig->Parameterized->EnableReshard.value_or(enableParameterizedByDefault)) {
        return false;
    }

    // So far, balancing via reshard only works if the desired tablet count is known.
    if (!TableConfig->DesiredTabletCount) {
        return false;
    }

    if (groupName == DefaultGroupName) {
        return TableConfig->EnableParameterized.value_or(bundleConfig->EnableParameterizedByDefault);
    }

    return TableConfig->EnableParameterized.value_or(true);
}

bool TTable::IsLegacyMoveBalancingEnabled() const
{
    if (!TableConfig->EnableAutoTabletMove) {
        return false;
    }

    auto groupName = GetBalancingGroup();
    if (!groupName) {
        return false;
    }

    const auto& groupConfig = GetOrCrash(Bundle->Config->Groups, *groupName);
    return groupConfig->Type == EBalancingType::Legacy && groupConfig->EnableMove;
}

THashMap<TClusterName, std::vector<NYPath::TYPath>> TTable::GetReplicaBalancingMinorTables(
    const std::string& selfClusterName) const
{
    if (!IsParameterizedMoveBalancingEnabled()) {
        return {};
    }

    auto groupName = GetBalancingGroup();
    YT_VERIFY(groupName);

    const auto& bundleConfig = Bundle->Config;
    const auto& groupConfig = GetOrCrash(bundleConfig->Groups, *groupName);
    if (groupConfig->Parameterized->ReplicaClusters.empty()) {
        return {};
    }

    THashMap<TClusterName, std::vector<NYPath::TYPath>> clusterToMinorTables;
    const auto& replicaPathOverrides = TableConfig->ReplicaPathOverrides;
    for (const auto& cluster : groupConfig->Parameterized->ReplicaClusters) {
        if (auto it = replicaPathOverrides.find(cluster); it != replicaPathOverrides.end()) {
            if (cluster == selfClusterName) {
                if (std::ranges::count(it->second, Path) == 0) {
                    THROW_ERROR_EXCEPTION("Replica path overrides for a table must contain this table")
                        << TErrorAttribute("cluster", cluster)
                        << TErrorAttribute("table_path", Path)
                        << TErrorAttribute("overrides", it->second);
                }
            }

            auto minorTablesIt = EmplaceOrCrash(clusterToMinorTables, cluster, std::vector<NYPath::TYPath>{});
            std::copy_if(it->second.begin(), it->second.end(), minorTablesIt->second.begin(), [&] (const auto& path) {
                return path != Path || cluster != selfClusterName;
            });
        } else if (cluster != selfClusterName) {
            EmplaceOrCrash(clusterToMinorTables, cluster, std::vector{Path});
        }
    }

    return clusterToMinorTables;
}

////////////////////////////////////////////////////////////////////////////////

TAlienTable::TAlienTable(
    TYPath path,
    TTableId tableId,
    TCellTag cellTag)
    : TTableBase(
        std::move(path),
        tableId,
        cellTag)
{ }

////////////////////////////////////////////////////////////////////////////////

std::optional<TGroupName> GetBalancingGroup(
    EInMemoryMode inMemoryMode,
    const TTableTabletBalancerConfigPtr& tableConfig,
    const TBundleTabletBalancerConfigPtr& bundleConfig)
{
    YT_VERIFY(tableConfig);
    YT_VERIFY(bundleConfig);

    if (tableConfig->Group.has_value()) {
        return bundleConfig->Groups.contains(*tableConfig->Group)
            ? tableConfig->Group
            : std::nullopt;
    }

    if (tableConfig->EnableParameterized.value_or(bundleConfig->EnableParameterizedByDefault)) {
        if (inMemoryMode != EInMemoryMode::None &&
            bundleConfig->DefaultInMemoryGroup.has_value())
        {
            return bundleConfig->Groups.contains(*bundleConfig->DefaultInMemoryGroup)
                ? bundleConfig->DefaultInMemoryGroup
                : std::nullopt;
        }
        return DefaultGroupName;
    }

    return inMemoryMode == EInMemoryMode::None
        ? LegacyOrdinaryGroupName
        : LegacyInMemoryGroupName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
