#pragma once

#include <server/IServer.h>

#include <Poco/Net/TCPServerConnectionFactory.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(
    DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
