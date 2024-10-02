#pragma once

#include "public.h"
#include "generic_orm_service_proxy.h"

#include <yt/yt_proto/yt/orm/client/proto/generic_object_service.pb.h>

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

IOrmClientPtr CreateGenericOrmClient(NRpc::IRoamingChannelProviderPtr channelProvider, TString serviceName);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NClient::NGeneric
