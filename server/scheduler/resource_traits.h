#pragma once

#include "public.h"

#include <yp/client/api/proto/enums.pb.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Allocations of non-anonymous resource have identifiers.
bool IsAnonymousResource(EResourceKind kind);

// At most one resource object of one singleton kind is configured for any node.
bool IsSingletonResource(EResourceKind kind);

// Capacities vector of homogeneous resource consists of a single number.
bool IsHomogeneousResource(EResourceKind kind);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
