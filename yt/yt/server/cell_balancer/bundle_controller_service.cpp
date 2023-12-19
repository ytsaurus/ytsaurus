#include "bundle_controller_service.h"

#include "private.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_service_proxy.h>

namespace NYT::NBundleController {

using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerService
    : public TServiceBase
{
public:
    explicit TBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TBundleControllerServiceProxy::GetDescriptor(),
            NCellBalancer::BundleControllerLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        Y_UNUSED(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBundleConfig));
    }

private:
    NCellBalancer::IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, GetBundleConfig)
    {
        context->SetRequestInfo("BundleName: %v",
            request->bundle_name());

        response->set_bundle_name(request->bundle_name());

        auto* cpuLimits = response->mutable_cpu_limits();
        cpuLimits->set_lookup_thread_pool_size(16);
        cpuLimits->set_query_thread_pool_size(4);
        cpuLimits->set_write_thread_pool_size(10);

        auto* memoryLimits = response->mutable_memory_limits();
        memoryLimits->set_compressed_block_cache(17179869184);
        memoryLimits->set_key_filter_block_cache(1024);
        memoryLimits->set_lookup_row_cache(1024);
        memoryLimits->set_tablet_dynamic(10737418240);
        memoryLimits->set_tablet_static(10737418240);
        memoryLimits->set_uncompressed_block_cache(17179869184);
        memoryLimits->set_versioned_chunk_meta(10737418240);

        response->set_rpc_proxy_count(6);

        auto* rpcProxyResourceGuarantee = response->mutable_rpc_proxy_resource_guarantee();
        rpcProxyResourceGuarantee->set_memory(21474836480);
        rpcProxyResourceGuarantee->set_net(1090519040);
        rpcProxyResourceGuarantee->set_type("medium");
        rpcProxyResourceGuarantee->set_vcpu(10000);

        response->set_tablet_node_count(1);

        auto* tabletNodeResourceGuarantee = response->mutable_tablet_node_resource_guarantee();
        tabletNodeResourceGuarantee->set_memory(107374182400);
        tabletNodeResourceGuarantee->set_net(5368709120);
        tabletNodeResourceGuarantee->set_type("cpu_intensive");
        tabletNodeResourceGuarantee->set_vcpu(28000);

        context->Reply();
    }
};

IServicePtr CreateBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
{
    return New<TBundleControllerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
