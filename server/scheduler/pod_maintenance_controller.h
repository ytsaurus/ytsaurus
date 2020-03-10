#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodMaintenanceController
    : public TRefCounted
{
public:
    explicit TPodMaintenanceController(NMaster::TBootstrap* bootstrap);
    ~TPodMaintenanceController();

    void AbortEviction(const NCluster::TClusterPtr& cluster);
    void RequestEviction(const NCluster::TClusterPtr& cluster);

    void ResetMaintenance(const NCluster::TClusterPtr& cluster);
    void RequestMaintenance(const NCluster::TClusterPtr& cluster);
    void SyncInProgressMaintenance(const NCluster::TClusterPtr& cluster);

    void SyncNodeAlerts(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TPodMaintenanceController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
