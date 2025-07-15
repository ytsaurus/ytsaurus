#include "cypress_transaction_service.h"

#include "bootstrap.h"
#include "private.h"
#include "cypress_proxy_service_base.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/cypress_transaction_client/cypress_transaction_service_proxy.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressTransactionClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TCypressTransactionService
    : public TCypressProxyServiceBase
{
public:
    explicit TCypressTransactionService(IBootstrap* bootstrap)
        : TCypressProxyServiceBase(
            bootstrap,
            bootstrap->GetInvoker("CypressTransactionService"),
            TCypressTransactionServiceProxy::GetDescriptor(),
            CypressProxyLogger(),
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Connection_(bootstrap->GetNativeConnection())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingTransaction)
            .SetInvoker(GetSyncInvoker()));

        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);
    }

private:
    const NNative::IConnectionPtr Connection_;

    template <class F>
    requires requires (F f, const TCypressTransactionServiceProxy& proxy) { { f(proxy)->Invoke().AsVoid() } -> std::same_as<TFuture<void>>; }
    void ForwardRequestToMaster(
        auto context,
        F createRequest) const
    {
        context->SetIncrementalRequestInfo("OldRequestId: %v", context->GetRequestId());

        auto channel = GetTargetMasterPeerChannelOrThrow(context);
        auto proxy = TCypressTransactionServiceProxy(std::move(channel));

        // TODO(h0pless, kvk1920): Set the correct timeout here.
        proxy.SetDefaultTimeout(DefaultRpcRequestTimeout);

        auto request = createRequest(proxy);
        auto requestId = request->GetRequestId();

        context->SetRequestInfo("NewRequestId: %v", requestId);

        // Copy everything.
        request->CopyFrom(context->Request());
        request->Header().CopyFrom(context->GetRequestHeader());

        // But don't copy the old request ID.
        ToProto(request->Header().mutable_request_id(), requestId);

        context->ReplyFrom(
            request->Invoke()
            .Apply(BIND([] (const typename decltype(request->Invoke())::TValueType& rsp) {
                return rsp->GetResponseMessage();
            })));
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, StartTransaction)
    {
        auto createForwardingRequest = [] (const TCypressTransactionServiceProxy& proxy) {
            return proxy.StartTransaction();
        };

        ForwardRequestToMaster(std::move(context), createForwardingRequest);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, CommitTransaction)
    {
        auto createForwardingRequest = [] (const TCypressTransactionServiceProxy& proxy) {
            return proxy.CommitTransaction();
        };

        ForwardRequestToMaster(std::move(context), createForwardingRequest);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, AbortTransaction)
    {
        auto createForwardingRequest = [] (const TCypressTransactionServiceProxy& proxy) {
            return proxy.AbortTransaction();
        };

        ForwardRequestToMaster(std::move(context), createForwardingRequest);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressTransactionClient::NProto, PingTransaction)
    {
        auto createForwardingRequest = [] (const TCypressTransactionServiceProxy& proxy) {
            return proxy.PingTransaction();
        };

        ForwardRequestToMaster(std::move(context), createForwardingRequest);
    }
};


////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCypressTransactionService(IBootstrap* bootstrap)
{
    return New<TCypressTransactionService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
