#pragma once

#include "private.h"

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IServer.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::HTTPRequestHandlerFactoryPtr CreateHttpHandlerFactory(THost* host, DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
