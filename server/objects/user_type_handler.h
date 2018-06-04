#pragma once

#include "public.h"

#include <yp/server/master/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(NMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
