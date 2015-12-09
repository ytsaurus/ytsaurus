#pragma once

#include <yt/ytlib/election/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IElectionCallbacks)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

DECLARE_REFCOUNTED_CLASS(TElectionManager)

DECLARE_REFCOUNTED_CLASS(TElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
