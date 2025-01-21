#include "hunk_tablet.h"

#include <yt/yt/server/master/chunk_server/chunk_list.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

std::string THunkTablet::GetLowercaseObjectName() const
{
    return Format("hunk tablet %v", GetId());
}

std::string THunkTablet::GetCapitalizedObjectName() const
{
    return Format("Hunk tablet %v", GetId());
}

TTabletStatistics THunkTablet::GetTabletStatistics(bool /*fromAuxiliaryCell*/) const
{
    // TODO(akozhikhov): We cannot account chunk statistics here as they depend on whether
    // each chunk is sealed which is racy with regard to tablet manager logic.
    TTabletStatistics tabletStatistics;
    tabletStatistics.TabletCount = 1;
    return tabletStatistics;
}

void THunkTablet::ValidateReshard() const
{
    TBase::ValidateReshard();

    auto* chunkList = GetChunkList();
    if (chunkList->Statistics().ChunkCount > 0) {
        THROW_ERROR_EXCEPTION("Non-empty hunk tablet %v cannot participate in reshard",
            chunkList->GetId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
