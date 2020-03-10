#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TNodeMaintenanceController
    : public TRefCounted
{
public:
    explicit TNodeMaintenanceController(NMaster::TBootstrap* bootstrap);
    ~TNodeMaintenanceController();

    void Acknowledge(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TNodeMaintenanceController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
