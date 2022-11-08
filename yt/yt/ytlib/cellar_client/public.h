#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NCellarClient {

////////////////////////////////////////////////////////////////////////////////

//! All cells (tablet and chaos) are uniformly divided into |CellShardCount| shards.
// BEWARE: Changing this value requires reign promotion since rolling update
// is not possible.
constexpr int CellShardCount = 60;

DEFINE_ENUM(ECellarType,
    ((Tablet)          (0))
    ((Chaos)           (1))
);

constexpr int TypicalCellarSize = 10;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarClient
