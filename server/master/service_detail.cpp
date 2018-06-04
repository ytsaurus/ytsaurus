#include "service_detail.h"
#include "bootstrap.h"
#include "yt_connector.h"

namespace NYP {
namespace NServer {
namespace NMaster {

////////////////////////////////////////////////////////////////////////////////

TServiceBase::TServiceBase(
    TBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    NRpc::IAuthenticatorPtr authenticator)
    : NRpc::TServiceBase(
        bootstrap->GetWorkerPoolInvoker(),
        descriptor,
        logger,
        NRpc::NullRealmId,
        std::move(authenticator))
    , Bootstrap_(bootstrap)
{ }

void TServiceBase::BeforeInvoke(NRpc::IServiceContext* /*context*/)
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    if (!ytConnector->IsConnected()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected to YT");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NServer
} // namespace NYP
