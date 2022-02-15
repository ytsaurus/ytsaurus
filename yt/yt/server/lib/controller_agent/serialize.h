#pragma once

#include "public.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    // 21.3 start here
    ((HostObjects)                          (300701))
    ((FixSimpleSort)                        (300702))
    ((InputStreamPerRange)                  (300703))
    ((RemoteCopyNetworks)                   (300704))
    ((AddAccountIntoUserObject)             (300705))
    // 22.1 start here
    ((RefactorStatistics)                   (300801))
    ((AddTotalJobStatistics)                (300802))
    ((NeededResourcesByPoolTree)            (300803))
    ((DiskResourcesInSanityCheck)           (300804))
    ((AlertManager)                         (300805))
);

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
