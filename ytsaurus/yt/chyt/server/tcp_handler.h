#pragma once

#include "private.h"

#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(THost* host, DB::IServer& server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
