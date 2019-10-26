#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/client/api/misc/public.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterInterface,
    (Client)
    (SecureClient)
    (Agent)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi
