#include "tcp_handler.h"

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

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ClickHouseYtLogger;

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

    DBPoco::Net::TCPServerConnection* createConnection(
        const DBPoco::Net::StreamSocket& socket,
        DB::TCPServer& tcpServer) override;
};

////////////////////////////////////////////////////////////////////////////////

DBPoco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(
    const DBPoco::Net::StreamSocket& socket,
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
            const DBPoco::Net::StreamSocket& socket)
            : DB::TCPHandler(
                server,
                tcpServer,
                socket,
                false /*parse_proxy_protocol*/,
                "" /*server_display_name*/,
                "" /*host_name_*/)
            , Host_(host)
        { }

        void customizeContext(DB::ContextMutablePtr context) override
        {
            TSecondaryQueryHeaderPtr header;
            TQueryId queryId;
            TTraceContextPtr traceContext;

            switch (context->getClientInfo().query_kind) {
                case DB::ClientInfo::QueryKind::NO_QUERY: {
                    THROW_ERROR_EXCEPTION("Attempt to process an uninitialized query object");
                    break;
                }
                case DB::ClientInfo::QueryKind::INITIAL_QUERY: {
                    traceContext = New<TTraceContext>(TSpanContext{.TraceId = TTraceId::Create()}, "TcpHandler");
                    queryId = traceContext->GetTraceId();
                    auto queryIdStr = ToString(queryId);
                    context->setInitialQueryId(queryIdStr);
                    context->setCurrentQueryId(queryIdStr);
                    break;
                }
                case DB::ClientInfo::QueryKind::SECONDARY_QUERY: {
                    header = NYTree::ConvertTo<TSecondaryQueryHeaderPtr>(NYson::TYsonString(context->getClientInfo().current_query_id));
                    context->setCurrentQueryId(ToString(header->QueryId));
                    queryId = header->QueryId;
                    traceContext = New<TTraceContext>(*header->SpanContext, "TcpHandler");
                    break;
                }
            }

            traceContext->AddTag("chyt.instance_cookie", Host_->GetInstanceCookie());
            traceContext->AddTag("chyt.instance_address", Host_->GetConfig()->Address);

            auto user = context->getClientInfo().initial_user;
            context->setCurrentUserName(user);
            YT_TLOG_DEBUG("Preparing new user")
                .With("UserName", user);
            Host_->PrepareClickHouseUser(user);
            YT_TLOG_DEBUG("User prepared");

            SetupHostContext(
                Host_,
                context,
                queryId,
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
