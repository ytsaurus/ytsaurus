#pragma once

#include "private.h"

#include <Server/IServer.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(THost* host, DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
