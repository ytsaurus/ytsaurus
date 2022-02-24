#include "private.h"

namespace NYT::NHydra2 {

using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

TFls<TEpochId> CurrentEpochId;

TCurrentEpochIdGuard::TCurrentEpochIdGuard(TEpochId epochId)
{
    YT_VERIFY(!*CurrentEpochId);
    *CurrentEpochId = epochId;
}

TCurrentEpochIdGuard::~TCurrentEpochIdGuard()
{
    *CurrentEpochId = {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
