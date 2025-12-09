#pragma once

#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

void ReconstructSequoia(
    const TSequoiaReconstructorConfigPtr reconstructorConfig,
    const std::string& masterConfigPath,
    const std::string& snapshotPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
