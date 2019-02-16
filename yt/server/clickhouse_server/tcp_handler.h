#pragma once

#include "public.h"

#include <server/IServer.h>

#include <Poco/Net/TCPServerConnectionFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(TBootstrap* bootstrap, DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
