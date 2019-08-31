#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct INodeScore
    : public virtual TRefCounted
{
    virtual void ReconcileState(const NCluster::TClusterPtr& cluster) = 0;
    virtual TNodeScoreValue Compute(NCluster::TNode* node) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeScore)

////////////////////////////////////////////////////////////////////////////////

INodeScorePtr CreateNodeScore(
    TNodeScoreConfigPtr config,
    NCluster::IObjectFilterEvaluatorPtr nodeFilterEvaluator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
