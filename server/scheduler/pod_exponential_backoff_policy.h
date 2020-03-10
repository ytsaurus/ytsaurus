#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodExponentialBackoffPolicy
    : public TRefCounted
{
public:
    explicit TPodExponentialBackoffPolicy(TPodExponentialBackoffPolicyConfigPtr config);
    ~TPodExponentialBackoffPolicy();

    TDuration GetNextBackoffDuration(const NCluster::TPod* pod);

    void ReconcileState(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TPodExponentialBackoffPolicy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
