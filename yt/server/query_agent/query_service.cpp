#include "query_service.h"
#include "private.h"
#include "public.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/service_detail.h>

#include <core/compression/public.h>
#include <core/compression/helpers.h>

#include <ytlib/table_client/schemaful_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_service_proxy.h>
#include <ytlib/query_client/query_statistics.h>
#include <ytlib/query_client/callbacks.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/tablet_client/wire_protocol.h>

#include <server/query_agent/config.h>
#include <server/query_agent/helpers.h>

#include <server/tablet_node/security_manager.h>

#include <server/cell_node/bootstrap.h>

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
            .SetCancelable(true)
            .SetEnableReorder(true));
    }

private:
    const TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        auto planFragment = FromProto(request->plan_fragment());

        context->SetRequestInfo("FragmentId: %v", planFragment->Query->Id);

        const auto& user = context->GetUser();
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        ExecuteRequestWithRetries(
            Config_->MaxQueryRetries,
            Logger,
            [&] () {
                TWireProtocolWriter protocolWriter;
                auto rowsetWriter = protocolWriter.CreateSchemafulRowsetWriter();

                auto executor = Bootstrap_->GetQueryExecutor();
                auto result = WaitFor(executor->Execute(planFragment, rowsetWriter))
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

