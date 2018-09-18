#pragma once

#include <yt/core/misc/guid.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

using TEpochId = TGuid;
using TPeerPriority = i64;

using TPeerId = int;
constexpr TPeerId InvalidPeerId = -1;

using TCellId = TGuid;
extern const TCellId NullCellId;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((InvalidState)  (800))
    ((InvalidLeader) (801))
    ((InvalidEpoch)  (802))
);

DEFINE_ENUM(EPeerState,
    (Stopped)
    (Voting)
    (Leading)
    (Following)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
