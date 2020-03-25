#include "tcp_handler.h"

#include "query_context.h"

#include <server/TCPHandler.h>

#include <util/string/cast.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTracing;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

class TTcpHandlerFactory
    : public Poco::Net::TCPServerConnectionFactory
{
private:
    TBootstrap* Bootstrap_;
    IServer& Server;

public:
    TTcpHandlerFactory(TBootstrap* bootstrap, IServer& server)
        : Bootstrap_(bootstrap)
        , Server(server)
    {}

    Poco::Net::TCPServerConnection* createConnection(
        const Poco::Net::StreamSocket& socket) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::TCPServerConnection* TTcpHandlerFactory::createConnection(
    const Poco::Net::StreamSocket& socket)
{
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
            context.getClientInfo().current_user = context.getClientInfo().initial_user;
            TQueryId queryId;
            TSpanContext parentSpan;
            auto& clientInfo = context.getClientInfo();

            TTraceContextPtr traceContext;

            // For secondary queries, query id looks like <query_id>@<parent_span_id><parent_sampled>.
            // Parent trace id is the same as client info initial_query_id.
            if (static_cast<int>(context.getClientInfo().query_kind) == 2 /* secondary query */) {
                auto requestCompositeQueryId = clientInfo.current_query_id;
                auto requestInitialQueryId = clientInfo.initial_query_id;
                YT_LOG_DEBUG("Parsing composite query id and initial query id (RequestCompositeQueryId: %v, RequestInitialQueryId: %v)",
                    requestCompositeQueryId,
                    requestInitialQueryId);
                auto pos = requestCompositeQueryId.find('@');
                YT_VERIFY(pos != TString::npos);
                auto requestQueryId = requestCompositeQueryId.substr(0, pos);
                auto requestParentSpanId = requestCompositeQueryId.substr(pos + 1, requestCompositeQueryId.size() - pos - 2);
                auto requestParentSampled = requestCompositeQueryId.back();
                YT_VERIFY(TQueryId::FromString(requestQueryId, &queryId));
                YT_VERIFY(TryIntFromString<16>(requestParentSpanId, parentSpan.SpanId));
                YT_VERIFY(TTraceId::FromString(requestInitialQueryId, &parentSpan.TraceId));
                YT_VERIFY(requestParentSampled == 'T' || requestParentSampled == 'F');
                context.getClientInfo().current_query_id = ToString(requestQueryId);
                YT_LOG_INFO(
                    "Query is secondary; composite query id successfully decomposed, actual query id substituted into the context "
                    "(CompositeQueryId: %v, QueryId: %v, ParentSpanId: %v, ParentSampled: %v, ParentTraceIdAkaInitialQueryId: %v)",
                    requestCompositeQueryId,
                    requestQueryId,
                    requestParentSpanId,
                    requestParentSampled,
                    requestInitialQueryId);
                traceContext = New<TTraceContext>(parentSpan, "TcpHandler");
                if (requestParentSampled == 'T') {
                    traceContext->SetSampled();
                }
            } else {
                auto requestQueryId = clientInfo.current_query_id;
                parentSpan = TSpanContext{TTraceId::Create(), InvalidSpanId, false, false};
                if (!TQueryId::FromString(requestQueryId, &queryId)) {
                    YT_LOG_INFO(
                        "Query is initial; query id from TCP handler is not a valid YT query id, "
                        "generating our own query id (RequestQueryId: %Qv, QueryId: %v)",
                        requestQueryId,
                        queryId);
                } else {
                    YT_LOG_INFO("Query is initial; query id from TCP handler is a valid YT query id (RequestQueryId: %Qv)", requestQueryId);
                }
            }

            SetupHostContext(Bootstrap_, context, queryId, std::move(traceContext));
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
