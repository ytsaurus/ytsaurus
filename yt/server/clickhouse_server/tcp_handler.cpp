#include "tcp_handler.h"

#include "storage.h"

#include <server/TCPHandler.h>

#include <common/logger_useful.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TTcpHandlerFactory
    : public Poco::Net::TCPServerConnectionFactory
{
private:
    TBootstrap* Bootstrap_;
    IServer& Server;
    Logger* Log;

public:
    TTcpHandlerFactory(TBootstrap* bootstrap, IServer& server)
        : Bootstrap_(bootstrap)
        , Server(server)
        , Log(&Logger::get("TCPHandlerFactory"))
    {}

    Poco::Net::TCPServerConnection* createConnection(
        const Poco::Net::StreamSocket& socket) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(
    const Poco::Net::StreamSocket& socket)
{
    LOG_TRACE(Log, "TCP Request. "
        << "Address: " << socket.peerAddress().toString());

    class TTcpHandler
        : public DB::TCPHandler
    {
    public:
        TTcpHandler(TBootstrap* bootstrap, DB::IServer& server, const Poco::Net::StreamSocket& socket)
            : DB::TCPHandler(server, socket)
            , Bootstrap_(bootstrap)
        { }

        virtual void customizeContext(DB::Context& context) override
        {
            SetupHostContext(Bootstrap_, context);
        }

    private:
        TBootstrap* const Bootstrap_;
    };

    return new TTcpHandler(Bootstrap_, Server, socket);
}

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(TBootstrap* bootstrap, IServer& server)
{
    return new TTcpHandlerFactory(bootstrap, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
