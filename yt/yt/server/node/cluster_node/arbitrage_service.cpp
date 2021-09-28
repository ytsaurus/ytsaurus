#include "arbitrage_service.h"

#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"
#include "private.h"

#include <yt/yt/ytlib/arbitrage_service/arbitrage_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NClusterNode {

using namespace NArbitrageClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf CpuResourceName = "cpu";
static constexpr TStringBuf MemoryResourceName = "memory";

////////////////////////////////////////////////////////////////////////////////

class TArbitrageService
    : public TServiceBase
{
public:
    explicit TArbitrageService(IBootstrapBase* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TArbitrageServiceProxy::GetDescriptor(),
            ClusterNodeLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetTarget));
    }

    DECLARE_RPC_SERVICE_METHOD(NArbitrageClient::NProto, GetStatus)
    {
        auto setResourceStatus = [&] (TStringBuf kind, i64 usage) {
            auto* resource = response->add_resources();
            resource->set_kind(ToString(kind));
            resource->set_usage(usage);
            // TODO(gritukan): Implement demand estimation.
            resource->set_demand(0);
        };

        const auto& nodeResourceManager = Bootstrap_->GetNodeResourceManager();
        auto cpuUsage = std::ceil(nodeResourceManager->GetCpuUsage());
        auto memoryUsage = nodeResourceManager->GetMemoryUsage();

        context->SetResponseInfo("CpuUsage: %v, MemoryUsage: %v",
            cpuUsage,
            memoryUsage);

        setResourceStatus(CpuResourceName, cpuUsage);
        setResourceStatus(MemoryResourceName, memoryUsage);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NArbitrageClient::NProto, SetTarget)
    {
        const auto& dynamicConfig = Bootstrap_->GetDynamicConfigManager()->GetConfig();
        if (dynamicConfig->ResourceLimits->UseInstanceLimitsTracker) {
            THROW_ERROR_EXCEPTION("Cannot set resource targets via arbiter: instance limits tracker is on");
        }

        std::optional<i64> cpuLimit;
        std::optional<i64> memoryLimit;
        for (const auto& resource : request->resources()) {
            if (resource.kind() == CpuResourceName) {
                cpuLimit = resource.target();
            } else if (resource.kind() == MemoryResourceName) {
                memoryLimit = resource.target();
            } else {
                YT_LOG_DEBUG("Unknown resource limit target is set (Kind: %v, Target: %v)",
                    resource.kind(),
                    resource.target());
            }
        }

        context->SetRequestInfo("CpuLimit: %v, MemoryLimit: %v",
            cpuLimit,
            memoryLimit);

        if (!cpuLimit || !memoryLimit) {
            THROW_ERROR_EXCEPTION("Both CPU and memory limits should be present");
        }

        const auto& nodeResourceManager = Bootstrap_->GetNodeResourceManager();
        nodeResourceManager->OnInstanceLimitsUpdated(*cpuLimit, *memoryLimit);

        context->Reply();
    }

private:
    IBootstrapBase* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateArbitrageService(IBootstrapBase* bootstrap)
{
    return New<TArbitrageService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
