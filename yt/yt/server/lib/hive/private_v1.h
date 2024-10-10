#pragma once

#include "public.h"

namespace NYT::NHiveServer::NV1 {

////////////////////////////////////////////////////////////////////////////////

class TMailbox;
DECLARE_ENTITY_TYPE(TCellMailbox, TCellId, ::THash<TCellId>)
DECLARE_ENTITY_TYPE(TAvenueMailbox, TAvenueEndpointId, ::THash<TAvenueEndpointId>)

DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV1
