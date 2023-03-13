
#include "config.h"
#include "table.h"
#include "tablet_cell_bundle.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TTable::TTable(
    bool sorted,
    NYPath::TYPath path,
    NObjectClient::TCellTag cellTag,
    TTableId tableId,
    TTabletCellBundle* bundle)
    : Sorted(sorted)
    , Path(std::move(path))
    , ExternalCellTag(cellTag)
    , Bundle(std::move(bundle))
    , Id(tableId)
{ }

std::optional<TGroupName> TTable::GetBalancingGroup() const
{
    const auto& bundleConfig = Bundle->Config;
    if (TableConfig->Group.has_value()) {
        return bundleConfig->Groups.contains(*TableConfig->Group)
            ? TableConfig->Group
            : std::nullopt;
    }

    if (TableConfig->EnableParameterized.value_or(bundleConfig->EnableParameterizedByDefault)) {
        if (InMemoryMode != EInMemoryMode::None &&
            bundleConfig->DefaultInMemoryGroup.has_value())
        {
            return bundleConfig->Groups.contains(*bundleConfig->DefaultInMemoryGroup)
                ? bundleConfig->DefaultInMemoryGroup
                : std::nullopt;
        }
        return DefaultGroupName;
    }

    return InMemoryMode == EInMemoryMode::None
        ? LegacyGroupName
        : LegacyInMemoryGroupName;
}

bool TTable::IsParameterizedBalancingEnabled() const
{
    if (!TableConfig->EnableAutoTabletMove) {
        return false;
    }

    const auto& groupName = GetBalancingGroup();

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

bool TTable::IsLegacyMoveBalancingEnabled() const
{
    if (!TableConfig->EnableAutoTabletMove) {
        return false;
    }

    const auto& groupName = GetBalancingGroup();
    if (!groupName) {
        return false;
    }

    const auto& groupConfig = GetOrCrash(Bundle->Config->Groups, *groupName);
    return groupConfig->Type == EBalancingType::Legacy && groupConfig->EnableMove;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
