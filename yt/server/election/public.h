#pragma once

#include <core/misc/public.h>

#include <ytlib/election/public.h>

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
