#pragma once

#include "common.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

struct TCellConfig;
typedef TIntrusivePtr<TCellConfig> TCellConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
