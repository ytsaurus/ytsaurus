#pragma once

#include "private.h"

#include <server/IServer.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(TBootstrap* bootstrap, DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
