#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/serialize.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELeaseManagerReign,
    ((StartingReign)                               (0)) // h0pless
    ((PersistentLeaseOwnerCellId)                  (1)) // h0pless
);

static_assert(TEnumTraits<ELeaseManagerReign>::IsMonotonic, "Lease manager reign enum is not monotonic");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
