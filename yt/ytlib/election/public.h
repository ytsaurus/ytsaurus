#pragma once

#include "common.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

struct TCellConfig;
typedef TIntrusivePtr<TCellConfig> TCellConfigPtr;

DECLARE_ENUM(EPeerState,
    (Stopped)
    (Voting)
    (Leading)
    (Following)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
