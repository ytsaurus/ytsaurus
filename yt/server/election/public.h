#pragma once

#include <core/misc/common.h>
#include <core/misc/guid.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerConfig;
typedef TIntrusivePtr<TElectionManagerConfig> TElectionManagerConfigPtr;

struct IElectionCallbacks;
typedef TIntrusivePtr<IElectionCallbacks> IElectionCallbacksPtr;

struct TEpochContext;
typedef TIntrusivePtr<TEpochContext> TEpochContextPtr;

class TElectionManager;
typedef TIntrusivePtr<TElectionManager> TElectionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
