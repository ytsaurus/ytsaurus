#pragma once

#include <server/IServer.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(
    DB::IServer& server);

}   // namespace NClickHouse
}   // namespace NYT
