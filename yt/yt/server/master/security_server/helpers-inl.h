#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_cell_group.h>
#include <yt/yt/server/master/cell_master/master_cell_group_manager.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
THashMap<std::string, T> CellTagMapToCellNameMap(const THashMap<NObjectClient::TCellTag, T>& map, const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    THashMap<std::string, T> result;
    for (const auto& [cellTag, value] : map) {
        auto cellName = multicellManager->GetMasterCellName(cellTag);
        YT_VERIFY(result.emplace(cellName, value).second);
    }
    return result;
}

template <class T>
THashMap<NObjectClient::TCellTag, T> CellNameMapToCellTagMapOrThrow(const THashMap<std::string, T>& map, const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    THashMap<NObjectClient::TCellTag, T> result;
    for (const auto& [cellName, value] : map) {
        auto optionalCellTag = multicellManager->FindMasterCellTagByName(cellName);
        if (!optionalCellTag) {
            THROW_ERROR_EXCEPTION("Invalid cell name %Qv", cellName);
        }
        YT_VERIFY(result.emplace(*optionalCellTag, value).second);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class TSerializeGroup, class TIncludeGroup>
void SerializeAccountResources(
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    TSerializeGroup serializeGroup,
    TIncludeGroup includeGroup)
{
    using namespace NYTree;

    const auto& groupManager = bootstrap->GetMasterCellGroupManager();
    auto universumGroupId = NCellMaster::GetUniversumGroupId(bootstrap->GetMulticellManager()->GetPrimaryCellTag());
    auto groups = GetValuesSortedByKey(groupManager->MasterCellGroups());
    std::erase_if(groups, [&] (auto* group) {
        return group->GetId() == universumGroupId || !includeGroup(group->GetId());
    });

    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (TFluentMap fluent) {
                serializeGroup(universumGroupId, fluent);
            })
            .DoIf(!groups.empty(), [&] (TFluentMap fluent) {
                fluent.Item("per_group").DoMapFor(groups, [&] (TFluentMap fluent, auto* group) {
                    fluent.Item(group->GetName()).DoMap([&] (TFluentMap fluent) {
                        serializeGroup(group->GetId(), fluent);
                    });
                });
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
