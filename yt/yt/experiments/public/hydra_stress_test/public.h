#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAutomatonPart)
DECLARE_REFCOUNTED_CLASS(TAutomaton)
DECLARE_REFCOUNTED_CLASS(TPeerChannel)
DECLARE_REFCOUNTED_CLASS(TChannelManager)
DECLARE_REFCOUNTED_CLASS(TConsistencyChecker)
DECLARE_REFCOUNTED_CLASS(TLinearizabilityChecker)
DECLARE_REFCOUNTED_CLASS(TLivenessChecker)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TConfig)
DECLARE_REFCOUNTED_CLASS(TSnapshotBuilder)
DECLARE_REFCOUNTED_CLASS(TPersistenceDestroyer)
DECLARE_REFCOUNTED_CLASS(TNetworkDisruptor)
DECLARE_REFCOUNTED_CLASS(TPeer)
DECLARE_REFCOUNTED_CLASS(TPeerService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
