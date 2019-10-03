#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    ((JobMetricsJobStateFilter)           (300195))
    // 19.7 starts here
    ((ExternalizedTransactions)           (300200))
    ((DynamicTableWriterConfig)           (300201))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
