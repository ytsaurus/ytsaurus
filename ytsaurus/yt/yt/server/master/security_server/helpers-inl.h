#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/server/master/cell_master/multicell_manager.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
THashMap<TString, T> CellTagMapToCellNameMap(const THashMap<NObjectClient::TCellTag, T>& map, const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    THashMap<TString, T> result;
    for (const auto& [cellTag, value] : map) {
        auto cellName = multicellManager->GetMasterCellName(cellTag);
        YT_VERIFY(result.emplace(cellName, value).second);
    }
    return result;
}

template <class T>
THashMap<NObjectClient::TCellTag, T> CellNameMapToCellTagMapOrThrow(const THashMap<TString, T>& map, const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    THashMap<NObjectClient::TCellTag, T> result;
    for (const auto& [cellName, value] : map) {
        auto optionalCellTag = multicellManager->FindMasterCellTagByName(cellName);
        if (!optionalCellTag) {
            THROW_ERROR_EXCEPTION("Invalid cell name %v", cellName);
        }
        YT_VERIFY(result.emplace(*optionalCellTag, value).second);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
