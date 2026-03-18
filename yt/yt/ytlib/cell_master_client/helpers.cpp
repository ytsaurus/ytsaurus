#include "helpers.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt_proto/yt/client/cell_master/proto/cell_directory.pb.h>

namespace NYT::NCellMasterClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool ClusterMasterCompositionChanged(
    const TSecondaryMasterConnectionConfigs& oldSecondaryMasterConnectionConfigs,
    const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs)
{
    if (newSecondaryMasterConnectionConfigs.size() != oldSecondaryMasterConnectionConfigs.size()) {
        return true;
    }

    for (const auto& [cellTag, newSecondaryMasterConnectionConfig] : newSecondaryMasterConnectionConfigs) {
        if (!oldSecondaryMasterConnectionConfigs.contains(cellTag)) {
            return true;
        }
        const auto& oldSecondaryMasterConnectionConfig = GetOrCrash(oldSecondaryMasterConnectionConfigs, cellTag);
        // TODO(cherepashka): replace with connection config comparison after changing NProto::TCellDirectory.
        if (oldSecondaryMasterConnectionConfig->Addresses != newSecondaryMasterConnectionConfig->Addresses) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TCellTagList GetMasterCellTags(const TSecondaryMasterConnectionConfigs& masterConnectionConfigs)
{
    auto cellTags = GetKeys(masterConnectionConfigs);
    return TCellTagList(cellTags.begin(), cellTags.end());
}

THashSet<TCellId> GetMasterCellIds(const TSecondaryMasterConnectionConfigs& masterConnectionConfigs)
{
    THashSet<TCellId> masterCellIds;
    for (const auto& [_, config] : masterConnectionConfigs) {
        InsertOrCrash(masterCellIds, config->CellId);
    }
    return masterCellIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
