#pragma once

#include "public.h"

namespace NYT::NHiveServer::NV2 {

////////////////////////////////////////////////////////////////////////////////

class TMailbox;
DECLARE_ENTITY_TYPE(TCellMailbox, TCellId, ::THash<TCellId>)
DECLARE_ENTITY_TYPE(TAvenueMailbox, TAvenueEndpointId, ::THash<TAvenueEndpointId>)

DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)
DECLARE_REFCOUNTED_STRUCT(TCellMailboxRuntimeData)
DECLARE_REFCOUNTED_STRUCT(TAvenueMailboxRuntimeData)

DECLARE_REFCOUNTED_CLASS(TPersistentMailboxState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV2
