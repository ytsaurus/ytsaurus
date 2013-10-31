#include "query_agent_service.h"
#include "private.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/new_table_client/chunk_writer.h>

#include <ytlib/query_client/query_fragment.h>

#include <core/compression/public.h>

#include <core/concurrency/action_queue.h>

#include <server/cell_node/bootstrap.h>

#include <server/query_agent/query_manager.h>

namespace NYT {
namespace NQueryAgent {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NQueryClient::NProto;
using namespace NRpc;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;
static auto& Profiler = QueryAgentProfiler;
static auto ProfilingPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TQueryAgentService::TQueryAgentService(TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        CreatePrioritizedInvoker(bootstrap->GetControlInvoker()),
        TProxy::GetServiceName(),
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

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TQueryAgentService, Execute)
{
    auto fragment = NQueryClient::FromProto(request->fragment());

    auto memoryWriter = New<TMemoryWriter>();
    auto chunkWriter = New<TChunkWriter>(
        ChunkWriterConfig_,
        EncodingWriterOptions_,
        memoryWriter);

    Bootstrap->GetQueryManager()
        ->Execute(fragment, chunkWriter)
        .Apply(BIND([=] (TError error) {
            if (error.IsOK()) {
                ToProto(response->mutable_chunk_meta(), memoryWriter->GetChunkMeta());
                response->Attachments() = std::move(memoryWriter->GetBlocks());
                context->Reply();
            } else {
                context->Reply(error);
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

