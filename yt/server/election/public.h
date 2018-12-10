#pragma once

#include <yt/ytlib/election/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IElectionCallbacks)
DECLARE_REFCOUNTED_STRUCT(IElectionManager)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

DECLARE_REFCOUNTED_CLASS(TDistributedElectionManager)
DECLARE_REFCOUNTED_CLASS(TElectionManagerThunk)

DECLARE_REFCOUNTED_CLASS(TDistributedElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
