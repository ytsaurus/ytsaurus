#pragma once

#include <server/IServer.h>

#include <Poco/Net/TCPServerConnectionFactory.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(
    DB::IServer& server);

}   // namespace NClickHouse
}   // namespace NYT
