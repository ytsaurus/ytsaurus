#include "dns_over_rpc_service.h"

#include "private.h"

#include <yt/yt/library/dns_over_rpc/client/dns_over_rpc_service_proxy.h>
#include <yt/yt/library/dns_over_rpc/client/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/dns/dns_resolver.h>

namespace NYT::NDns {

using namespace NNet;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TDnsOverRpcService
    : public NRpc::TServiceBase
{
public:
    TDnsOverRpcService(
        IDnsResolverPtr resolver,
        IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TDnsOverRpcServiceProxy::GetDescriptor(),
            DnsLogger)
        , Resolver_(std::move(resolver))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Resolve)
            .SetConcurrencyLimit(1'000'000));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Resolve)
    {
        context->SetRequestInfo("Count: %v", request->subrequests_size());

        std::vector<TFuture<TNetworkAddress>> futures;
        futures.reserve(request->subrequests_size());
        for (const auto& subrequest : request->subrequests()) {
            futures.push_back(Resolver_->Resolve(
                subrequest.host_name(),
                FromProto<TDnsResolveOptions>(subrequest.options())));
        }

        AllSet(std::move(futures))
            .Subscribe(BIND([=] (const TErrorOr<std::vector<TErrorOr<TNetworkAddress>>>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                    return;
                }

                const auto& addressesOrErrors = result.Value();
                for (const auto& addressOrError : addressesOrErrors) {
                    auto* subresponse = response->add_subresponses();
                    if (addressOrError.IsOK()) {
                        const auto& address = addressOrError.Value();
                        ToProto(subresponse->mutable_address(), address);
                    } else {
                        ToProto(subresponse->mutable_error(), addressOrError);
                    }
                }

                context->Reply();
            }).Via(GetDefaultInvoker()));
    }

private:
    const IDnsResolverPtr Resolver_;
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateDnsOverRpcService(
    IDnsResolverPtr resolver,
    IInvokerPtr invoker)
{
    return New<TDnsOverRpcService>(
        std::move(resolver),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
