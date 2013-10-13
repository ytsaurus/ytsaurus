#pragma once

#include <core/misc/common.h>
#include <core/misc/guid.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

class TCellConfig;
typedef TIntrusivePtr<TCellConfig> TCellConfigPtr;

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TEpochId;
typedef i64 TPeerPriority;

typedef int TPeerId;
const TPeerId InvalidPeerId = -1;

typedef TGuid TCellGuid;
extern const TCellGuid NullCellGuid;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((InvalidState)  (800))
    ((InvalidLeader) (801))
    ((InvalidEpoch)  (802))
);

DECLARE_ENUM(EPeerState,
    (Stopped)
    (Voting)
    (Leading)
    (Following)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
