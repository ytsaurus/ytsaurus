#pragma once

#include <yt/yt/library/profiling/solomon/proxy.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TFlowEndpointProvider
    : public NProfiling::IEndpointProvider
{
public:
    TFlowEndpointProvider(int monitoringPort, int companionMonitoringPort);

    std::string GetComponentName() const override;
    std::vector<TEndpoint> GetEndpoints() const override;

private:
    const int MonitoringPort_;
    const int CompanionMonitoringPort_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
