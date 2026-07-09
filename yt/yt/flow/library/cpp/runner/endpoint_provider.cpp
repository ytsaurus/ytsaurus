#include "endpoint_provider.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TFlowEndpointProvider::TFlowEndpointProvider(int monitoringPort, int companionMonitoringPort)
    : MonitoringPort_(monitoringPort)
    , CompanionMonitoringPort_(companionMonitoringPort)
{ }

std::string TFlowEndpointProvider::GetComponentName() const
{
    return "flow_node";
}

std::vector<NProfiling::IEndpointProvider::TEndpoint> TFlowEndpointProvider::GetEndpoints() const
{
    std::vector<TEndpoint> endpoints = {
        {
            .Name = "flow_node",
            .Address = Format("http://localhost:%v/solomon/all", MonitoringPort_),
        },
    };

    if (CompanionMonitoringPort_ != 0) {
        endpoints.push_back({
            .Name = "companion",
            .Address = Format("http://localhost:%v/metrics", CompanionMonitoringPort_),
        });
    }

    return endpoints;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
