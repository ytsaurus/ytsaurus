#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IPodNodeScore
    : public virtual TRefCounted
{
    virtual TPodNodeScoreValue Compute(NCluster::TNode* node, NCluster::TPod* pod) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPodNodeScore)

////////////////////////////////////////////////////////////////////////////////

IPodNodeScorePtr CreatePodNodeScore(TPodNodeScoreConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
