#include "query_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/query_agent/config.h>
#include <yt/server/query_agent/helpers.h>

#include <yt/server/tablet_node/security_manager.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/plan_fragment.h>
#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/query_statistics.h>

#include <yt/ytlib/table_client/schemaful_writer.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/compression/helpers.h>
#include <yt/core/compression/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NQueryAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NCompression;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TServiceBase
{
public:
    TQueryService(
        TQueryAgentConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            CreatePrioritizedInvoker(bootstrap->GetQueryPoolInvoker()),
            TQueryServiceProxy::GetServiceName(),
            QueryAgentLogger,
            TQueryServiceProxy::GetProtocolVersion())
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true));
    }

private:
    const TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        LOG_DEBUG("Deserializing subfragment");

        auto query = FromProto(request->query());
        auto options = FromProto(request->options());

        std::vector<TDataRanges> dataSources;
        dataSources.reserve(request->data_sources_size());
        for (int i = 0; i < request->data_sources_size(); ++i) {
            dataSources.push_back(FromProto(request->data_sources(i)));
        }

        LOG_DEBUG("Deserialized subfragment");

        context->SetRequestInfo("FragmentId: %v", query->Id);

        const auto& user = context->GetUser();
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        ExecuteRequestWithRetries(
            Config_->MaxQueryRetries,
            Logger,
            [&] () {
                TWireProtocolWriter protocolWriter;
                auto rowsetWriter = protocolWriter.CreateSchemafulRowsetWriter(
                    query->GetTableSchema());

                auto executor = Bootstrap_->GetQueryExecutor();
                auto result = WaitFor(executor->Execute(query, dataSources, rowsetWriter, options))
                    .ValueOrThrow();

                auto responseCodec = request->has_response_codec()
                    ? ECodec(request->response_codec())
                    : ECodec::None;
                response->Attachments() = CompressWithEnvelope(protocolWriter.Flush(), responseCodec);
                ToProto(response->mutable_query_statistics(), result);
                context->Reply();
            });
    }

};

IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryService>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

