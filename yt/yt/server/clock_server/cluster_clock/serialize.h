#pragma once

#include "public.h"

#include <yt/server/lib/hydra/public.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClockReign,
    ((TheBigBang)                                                 (1))  // savrus
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
