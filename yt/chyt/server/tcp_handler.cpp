#include "tcp_handler.h"

#include "helpers.h"
#include "host.h"
#include "query_context.h"
#include "secondary_query_header.h"

#include <Server/TCPHandler.h>
#include <Server/TCPServerConnectionFactory.h>

#include <util/string/cast.h>
#include <util/string/split.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTracing;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

class TTcpHandlerFactory
    : public DB::TCPServerConnectionFactory
{
private:
    THost* Host_;
    IServer& Server_;

public:
    TTcpHandlerFactory(THost* host, IServer& server)
        : Host_(host)
        , Server_(server)
    { }

    Poco::Net::TCPServerConnection* createConnection(
        const Poco::Net::StreamSocket& socket,
        DB::TCPServer& tcpServer) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(
    const Poco::Net::StreamSocket& socket,
    DB::TCPServer& tcpServer)
{
    class TTcpHandler
        : public DB::TCPHandler
    {
    public:
        TTcpHandler(
            THost* host,
            DB::IServer& server,
            DB::TCPServer& tcpServer,
            const Poco::Net::StreamSocket& socket)
            : DB::TCPHandler(
                server,
                tcpServer,
                socket,
                false /*parse_proxy_protocol*/,
                "" /*server_display_name*/)
            , Host_(host)
        { }

        void customizeContext(DB::ContextMutablePtr context) override
        {
            if (context->getClientInfo().query_kind != DB::ClientInfo::QueryKind::SECONDARY_QUERY) {
                // TODO(max42): support.
                THROW_ERROR_EXCEPTION("Queries via native TCP protocol are not supported (CHYT-342)");
            }

            auto user = context->getClientInfo().initial_user;

            context->setCurrentUserName(user);

            auto header = NYTree::ConvertTo<TSecondaryQueryHeaderPtr>(NYson::TYsonString(context->getClientInfo().current_query_id));
            context->setCurrentQueryId(ToString(header->QueryId));

            TTraceContextPtr traceContext = New<TTraceContext>(*header->SpanContext, "TcpHandler");
            traceContext->AddTag("chyt.instance_cookie", Host_->GetInstanceCookie());
            traceContext->AddTag("chyt.instance_address", Host_->GetConfig()->Address);

            YT_LOG_DEBUG("Registering new user (UserName: %v)", user);
            RegisterNewUser(
                context->getAccessControl(),
                TString(user),
                Host_->HasUserDefinedSqlObjectStorage());
            YT_LOG_DEBUG("User registered");

            SetupHostContext(
                Host_,
                context,
                header->QueryId,
                std::move(traceContext),
                /*dataLensRequestId*/ std::nullopt,
                /*yqlOperationId*/ std::nullopt,
                header);
        }

    private:
        THost* const Host_;
    };

    return new TTcpHandler(Host_, Server_, tcpServer, socket);
}

////////////////////////////////////////////////////////////////////////////////

DB::TCPServerConnectionFactory::Ptr CreateTcpHandlerFactory(THost* host, IServer& server)
{
    return new TTcpHandlerFactory(host, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
