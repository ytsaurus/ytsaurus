#pragma once

#include "common.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

class TCellConfig;
typedef TIntrusivePtr<TCellConfig> TCellConfigPtr;

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
