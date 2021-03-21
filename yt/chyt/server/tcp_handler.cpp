#include "tcp_handler.h"

#include "helpers.h"
#include "query_context.h"
#include "subquery_header.h"

#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPHandler.h>

#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTracing;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

class TTcpHandlerFactory
    : public Poco::Net::TCPServerConnectionFactory
{
private:
    THost* Host_;
    IServer& Server;

public:
    TTcpHandlerFactory(THost* host, IServer& server)
        : Host_(host)
        , Server(server)
    { }

    Poco::Net::TCPServerConnection* createConnection(const Poco::Net::StreamSocket& socket) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(const Poco::Net::StreamSocket& socket)
{
    class TTcpHandler
        : public DB::TCPHandler
    {
    public:
        TTcpHandler(THost* host, DB::IServer& server, const Poco::Net::StreamSocket& socket)
            : DB::TCPHandler(server, socket, false, {})
            , Host_(host)
        { }

        virtual void customizeContext(DB::Context& context) override
        {
            auto& clientInfo = context.getClientInfo();

            if (clientInfo.query_kind != DB::ClientInfo::QueryKind::SECONDARY_QUERY) {
                // TODO(max42): support.
                THROW_ERROR_EXCEPTION("Queries via native TCP protocol are not supported (CHYT-342)");
            }

            clientInfo.current_user = clientInfo.initial_user;

            auto header = NYTree::ConvertTo<TSubqueryHeaderPtr>(NYson::TYsonString(clientInfo.current_query_id));
            clientInfo.current_query_id = ToString(header->QueryId);

            TTraceContextPtr traceContext = New<TTraceContext>(header->SpanContext, "TcpHandler");

            YT_LOG_DEBUG("Registering new user (UserName: %v)", clientInfo.current_user);
            RegisterNewUser(context.getAccessControlManager(), TString(clientInfo.current_user));
            YT_LOG_DEBUG("User registered");

            SetupHostContext(
                Host_,
                context,
                header->QueryId,
                std::move(traceContext),
                /* dataLensRequestId */ std::nullopt,
                header);
        }

    private:
        THost* const Host_;
    };

    return new TTcpHandler(Host_, Server, socket);
}

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(THost* host, IServer& server)
{
    return new TTcpHandlerFactory(host, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
