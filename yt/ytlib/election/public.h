#pragma once

#include <core/misc/common.h>
#include <core/misc/guid.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellManager)
DECLARE_REFCOUNTED_CLASS(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TEpochId;
typedef i64 TPeerPriority;

typedef int TPeerId;
const TPeerId InvalidPeerId = -1;

typedef TGuid TCellId;
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
