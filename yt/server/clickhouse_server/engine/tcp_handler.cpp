#include "tcp_handler.h"

//#include <server/TCPHandler.h>

//#include <common/logger_useful.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TTcpHandlerFactory
    : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer& Server;
    Logger* Log;

public:
    TTcpHandlerFactory(IServer& server)
        : Server(server)
        , Log(&Logger::get("TCPHandlerFactory"))
    {}

    Poco::Net::TCPServerConnection* createConnection(
        const Poco::Net::StreamSocket& socket) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(
    const Poco::Net::StreamSocket& socket)
{
    CH_LOG_TRACE(Log, "TCP Request. "
        << "Address: " << socket.peerAddress().toString());

    return new TCPHandler(Server, socket);
}

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(IServer& server)
{
    return new TTcpHandlerFactory(server);
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
