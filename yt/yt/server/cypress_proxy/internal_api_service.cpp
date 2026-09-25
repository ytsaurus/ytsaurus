#include "internal_api_service.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/ytlib/api/internal_proxy/internal_api_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NInternalProxy;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TInternalApiService
    : public TServiceBase
{
public:
    explicit TInternalApiService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetInvoker("InternalApiService"),
            TInternalApiServiceProxy::GetDescriptor(),
            CypressProxyLogger().WithTag("Component", "InternalApiService"),
            TServiceOptions{.Authenticator = bootstrap->GetNativeAuthenticator()})
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableBalancingAttributes)
            .SetHeavy(true)
            .SetCancelable(true));
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, GetTableBalancingAttributes)
    {
        THROW_ERROR_EXCEPTION_UNLESS(request->has_cell_tag(), "External master cell tag is required");
        auto externalCellTag = FromProto<TCellTag>(request->cell_tag());
        context->SetRequestInfo("ExternalCellTag: %v, TableCount: %v, FetchBalancingAttributes: %v, FetchStatistics: %v",
            externalCellTag,
            request->table_ids_size(),
            request->fetch_balancing_attributes(),
            request->fetch_statistics());

        TMasterTabletServiceProxy proxy(Bootstrap_->GetNativeConnection()->GetMasterChannelOrThrow(
            EMasterChannelKind::Follower,
            externalCellTag));
        auto nativeRequest = proxy.GetTableBalancingAttributes();
        nativeRequest->SetResponseHeavy(true);
        nativeRequest->SetTimeout(context->GetTimeout());
        SetAuthenticationIdentity(nativeRequest, context->GetAuthenticationIdentity());

        nativeRequest->Swap(request);

        auto nativeResponse = WaitFor(nativeRequest->Invoke())
            .ValueOrThrow();
        response->Swap(nativeResponse.Get());
        context->SetResponseInfo("TableCount: %v", response->tables_size());
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateInternalApiService(IBootstrap* bootstrap)
{
    return New<TInternalApiService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
