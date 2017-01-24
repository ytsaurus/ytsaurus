#include "query_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/query_agent/config.h>
#include <yt/server/query_agent/helpers.h>

#include <yt/server/tablet_node/security_manager.h>
#include <yt/server/tablet_node/slot_manager.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/tablet_manager.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_service_proxy.h>
#include <yt/ytlib/query_client/query_statistics.h>
#include <yt/ytlib/query_client/functions_cache.h>

#include <yt/ytlib/table_client/schemaful_writer.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/compression/helpers.h>
#include <yt/core/compression/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NQueryAgent {

using namespace NYTree;
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
            bootstrap->GetQueryPoolInvoker(),
            TQueryServiceProxy::GetDescriptor(),
            QueryAgentLogger)
        , Config_(config)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetInvoker(bootstrap->GetLookupPoolInvoker()));
    }

private:
    const TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        LOG_DEBUG("Deserializing subfragment");

        auto query = FromProto<TConstQueryPtr>(request->query());
        context->SetRequestInfo("FragmentId: %v", query->Id);

        auto externalCGInfo = New<TExternalCGInfo>();
        FromProto(&externalCGInfo->Functions, request->external_functions());
        externalCGInfo->NodeDirectory->MergeFrom(request->node_directory());

        auto options = FromProto<TQueryOptions>(request->options());

        auto dataSources = FromProto<std::vector<TDataRanges>>(request->data_sources());

        LOG_DEBUG("Deserialized subfragment (FragmentId: %v, InputRowLimit: %v, OutputRowLimit: %v, "
            "RangeExpansionLimit: %v, MaxSubqueries: %v, EnableCodeCache: %v, WorkloadDescriptor: %v, "
            "DataRangeCount: %v)",
            query->Id,
            query->InputRowLimit,
            query->OutputRowLimit,
            options.RangeExpansionLimit,
            options.MaxSubqueries,
            options.EnableCodeCache,
            options.WorkloadDescriptor,
            dataSources.size());

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        ExecuteRequestWithRetries(
            Config_->MaxQueryRetries,
            Logger,
            [&] () {
                TWireProtocolWriter protocolWriter;
                auto rowsetWriter = protocolWriter.CreateSchemafulRowsetWriter(query->GetTableSchema());

                auto executor = Bootstrap_->GetQueryExecutor();

                auto result = WaitFor(executor->Execute(
                    query,
                    externalCGInfo,
                    dataSources,
                    rowsetWriter,
                    options))
                    .ValueOrThrow();

                auto responseCodec = request->has_response_codec()
                    ? ECodec(request->response_codec())
                    : ECodec::None;
                response->Attachments() = CompressWithEnvelope(protocolWriter.Flush(), responseCodec);
                ToProto(response->mutable_query_statistics(), result);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Read)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto timestamp = TTimestamp(request->timestamp());
        // TODO(sandello): Extract this out of RPC request.
        auto workloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
        auto requestData = DecompressWithEnvelope(request->Attachments());

        context->SetRequestInfo("TabletId: %v, Timestamp: %v",
            tabletId,
            timestamp);

        auto slotManager = Bootstrap_->GetTabletSlotManager();

        const auto& user = context->GetUser();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        ExecuteRequestWithRetries(
            Config_->MaxQueryRetries,
            Logger,
            [&] () {
                auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
                slotManager->ValidateTabletAccess(
                    tabletSnapshot,
                    EPermission::Read,
                    timestamp);

                tabletSnapshot->ValidateMountRevision(mountRevision);

                TWireProtocolReader reader(requestData);
                TWireProtocolWriter writer;

                const auto& tabletManager = tabletSnapshot->TabletManager;
                tabletManager->Read(
                    tabletSnapshot,
                    timestamp,
                    workloadDescriptor,
                    &reader,
                    &writer);

                auto responseData = writer.Flush();
                auto responseCodec = request->has_response_codec()
                    ? ECodec(request->response_codec())
                    : ECodec(ECodec::None);
                response->Attachments() = CompressWithEnvelope(responseData,  responseCodec);
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

