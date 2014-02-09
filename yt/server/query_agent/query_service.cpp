#include "query_service.h"
#include "private.h"

#include <core/compression/public.h>

#include <core/rpc/service_detail.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_chunk_writer.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <server/cell_node/bootstrap.h>

#include <server/query_agent/query_manager.h>

namespace NYT {
namespace NQueryAgent {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NRpc;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public NRpc::TServiceBase
{
public:
    explicit TQueryService(NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            CreatePrioritizedInvoker(bootstrap->GetControlInvoker()),
            TQueryServiceProxy::GetServiceName(),
            QueryAgentLogger.GetCategory())
        , Bootstrap(bootstrap)
    {
        YCHECK(bootstrap);

        ChunkWriterConfig_ = New<TChunkWriterConfig>();

        EncodingWriterOptions_ = New<TEncodingWriterOptions>();
        EncodingWriterOptions_->CompressionCodec = NCompression::ECodec::Lz4;

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetEnableReorder(true));
    }

private:
    NVersionedTableClient::TChunkWriterConfigPtr ChunkWriterConfig_;
    NChunkClient::TEncodingWriterOptionsPtr EncodingWriterOptions_;

    NCellNode::TBootstrap* Bootstrap;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        auto planFragment = FromProto(request->plan_fragment());
        planFragment.GetContext()->GetNodeDirectory()->MergeFrom(request->node_directory());

        auto memoryWriter = New<TMemoryWriter>();
        auto chunkWriter = CreateSchemedChunkWriter(
            ChunkWriterConfig_,
            EncodingWriterOptions_,
            memoryWriter);

        Bootstrap
            ->GetQueryManager()
            ->Execute(planFragment, chunkWriter)
            .Apply(BIND([=] (TError error) {
                if (error.IsOK()) {
                    response->mutable_chunk_meta()->Swap(&memoryWriter->GetChunkMeta());
                    response->Attachments() = std::move(memoryWriter->GetBlocks());
                    context->Reply();
                } else {
                    context->Reply(error);
                }
            }));
    }

};

IServicePtr CreateQueryService(NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

