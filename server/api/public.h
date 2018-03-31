#pragma once

#include <yp/server/misc/public.h>

namespace NYP {
namespace NServer {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterInterface,
    (Client)
    // COMPAT(babenko)
    (SecureClient)
    (Agent)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NServer
} // namespace NYP
