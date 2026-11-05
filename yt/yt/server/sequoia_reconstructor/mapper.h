#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void ExecuteSequoiaReconstructorMap(
    NCellMaster::TBootstrap* bootstrap,
    TRecordsConsumer* consumer,
    int jobCount,
    int jobIndex);

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IMapperBase> CreateYtMapper();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
