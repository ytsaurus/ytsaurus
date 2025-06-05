#include "cypress_proxy_service_base.h"

namespace NYT::NCypressProxy {

using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TCypressProxyServiceBase::TCypressProxyServiceBase(
    IBootstrap* bootstrap,
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    TLogger logger,
    TServiceOptions options)
    : TServiceBase(
        std::move(defaultInvoker),
        descriptor,
        std::move(logger),
        std::move(options))
    , Bootstrap_(bootstrap)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
