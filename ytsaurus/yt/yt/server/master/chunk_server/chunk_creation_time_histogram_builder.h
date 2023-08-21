#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

IMasterCellChunkStatisticsPieceCollectorPtr CreateChunkCreationTimeHistogramBuilder(
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
