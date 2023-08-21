#include "tvm_bridge_service.h"

#include "private.h"
#include "tvm_bridge_service_proxy.h"

#include <yt/yt/ytlib/auth/proto/tvm_bridge_service.pb.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NAuth {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TTvmBridgeService
    : public TServiceBase
{
public:
    TTvmBridgeService(
        IDynamicTvmServicePtr tvmService,
        IInvokerPtr invoker,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TTvmBridgeServiceProxy::GetDescriptor(),
            TvmBridgeLogger,
            NullRealmId,
            std::move(authenticator))
        , TvmService_(std::move(tvmService))
    {
        YT_VERIFY(TvmService_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(FetchTickets)
            .SetCancelable(true));
    }

private:
    IDynamicTvmServicePtr TvmService_;

    DECLARE_RPC_SERVICE_METHOD(NProto, FetchTickets)
    {
        auto destinations = FromProto<std::vector<ui32>>(request->destinations());

        context->SetRequestInfo("Source: %v, Destinations: %v",
            request->source(),
            MakeShrunkFormattableView(destinations, TDefaultFormatter(), 10));

        if (request->source() != TvmService_->GetSelfTvmId()) {
            THROW_ERROR_EXCEPTION("Cannot fetch tickets for provided source")
                << TErrorAttribute("source", request->source());
        }

        for (const auto& destination : request->destinations()) {
            auto* result = response->add_results();
            result->set_destination(destination);
            try {
                auto ticket = TvmService_->GetServiceTicket(destination);
                *result->mutable_ticket() = std::move(ticket);
            } catch (const TErrorException& ex) {
                YT_LOG_INFO("Could not fetch service ticket (Source: %v, Destination: %v)",
                    request->source(),
                    destination);
                ToProto(
                    result->mutable_error(),
                    TError("Could not fetch service ticket")
                        << ex
                        << TErrorAttribute("source", request->source())
                        << TErrorAttribute("destination", destination));
            }
        }

        context->SetResponseInfo("Source: %v, Destinations: %v",
            request->source(),
            MakeShrunkFormattableView(destinations, TDefaultFormatter(), 10));

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTvmBridgeService(
    IDynamicTvmServicePtr tvmService,
    IInvokerPtr invoker,
    IAuthenticatorPtr authenticator)
{
    return New<TTvmBridgeService>(
        std::move(tvmService),
        std::move(invoker),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
