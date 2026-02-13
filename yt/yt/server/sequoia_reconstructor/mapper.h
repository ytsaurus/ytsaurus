#pragma once

#include "record_consumer.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void ExecuteSequoiaReconstructorMapStage(
    NCellMaster::TBootstrap* bootstrap,
    TRecordsConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
