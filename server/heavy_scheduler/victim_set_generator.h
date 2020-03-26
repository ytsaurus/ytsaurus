#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IVictimSetGenerator
    : public TRefCounted
{
    // See description below.
    virtual std::vector<NCluster::TPod*> GetNextCandidates() = 0;
};

DEFINE_REFCOUNTED_TYPE(IVictimSetGenerator);

////////////////////////////////////////////////////////////////////////////////

//! Create new generator of given type. Every call to GetNextCandidates()
//! returns a distinct set of pods that satisfies the following constraints:
//! 1) All returned pods are scheduled to victimNode;
//! 2) Returned pods' resource vector sum (weakly) dominates starvingPod
//!    resource vector;
//! 3) disruptionThrottler does not throttle eviction of these pods.
//!
//! When generator is exhausted, it returns empty vector.
IVictimSetGeneratorPtr CreateNodeVictimSetGenerator(
    EVictimSetGeneratorType generatorType,
    NCluster::TNode* victimNode,
    NCluster::TPod* starvingPod,
    TDisruptionThrottlerPtr disruptionThrottler,
    bool verbose);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
