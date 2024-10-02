#include "service_detail.h"

#include "bootstrap.h"
#include "yt_connector.h"

#include <yt/yt/library/tracing/jaeger/sampler.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    NRpc::IAuthenticatorPtr authenticator,
    IInvokerPtr defaultInvoker)
    : NRpc::TServiceBase(
        defaultInvoker ? std::move(defaultInvoker) : bootstrap->GetWorkerPoolInvoker(),
        descriptor,
        logger,
        NRpc::TServiceOptions{
            .Authenticator = std::move(authenticator),
        })
    , Bootstrap_(bootstrap)
{ }

void TServiceBase::SetupTracing(const NYT::NRpc::IServiceContextPtr& context)
{
    auto traceContext = NTracing::TryGetCurrentTraceContext();
    if (!traceContext) {
        return;
    }

    const auto& identity = context->GetAuthenticationIdentity();
    Bootstrap_->GetTracingSampler()->SampleTraceContext(identity.User, traceContext);

    if (traceContext->IsRecorded()) {
        traceContext->AddTag("user", identity.User);
        if (identity.UserTag != identity.User) {
            traceContext->AddTag("user_tag", identity.UserTag);
        }
    }
}

void TServiceBase::BeforeInvoke(NRpc::IServiceContext* context)
{
    TBase::BeforeInvoke(context);

    Bootstrap_->GetYTConnector()->CheckConnected()
        .ThrowOnError();
    SetupTracing(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
