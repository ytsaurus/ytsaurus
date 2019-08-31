#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudgetController
    : public TRefCounted
{
public:
    TPodDisruptionBudgetController(
        NMaster::TBootstrap* bootstrap,
        TPodDisruptionBudgetControllerConfigPtr config);

    void Run(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TPodDisruptionBudgetController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
