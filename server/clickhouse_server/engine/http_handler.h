#pragma once

#include <server/IServer.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(
    DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
