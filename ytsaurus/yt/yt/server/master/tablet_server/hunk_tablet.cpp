#include "hunk_tablet.h"

#include <yt/yt/server/master/chunk_server/chunk_list.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

TString THunkTablet::GetLowercaseObjectName() const
{
    return Format("hunk tablet %v", GetId());
}

TString THunkTablet::GetCapitalizedObjectName() const
{
    return Format("Hunk tablet %v", GetId());
}

TTabletStatistics THunkTablet::GetTabletStatistics() const
{
    return {};
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
