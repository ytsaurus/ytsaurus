#include "generic_orm_service_proxy.h"

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

TGenericServiceProxy::TGenericServiceProxy(
    NRpc::IChannelPtr channel,
    const NRpc::TServiceDescriptor& descriptor)
    : TProxyBase(std::move(channel), descriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NGeneric
