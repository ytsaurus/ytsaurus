#include "query_service.h"
#include "private.h"

#include <core/concurrency/fiber.h>

#include <core/compression/public.h>

#include <core/rpc/service_detail.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_chunk_writer.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

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
    : public TServiceBase
{
public:
    explicit TQueryService(
        IInvokerPtr invoker,
        IExecutorPtr executor)
        : TServiceBase(
            CreatePrioritizedInvoker(invoker),
            TQueryServiceProxy::GetServiceName(),
            QueryAgentLogger.GetCategory())
        , Executor_(executor)
    {
        YCHECK(Executor_);

        ChunkWriterConfig_ = New<TChunkWriterConfig>();

        EncodingWriterOptions_ = New<TEncodingWriterOptions>();
        EncodingWriterOptions_->CompressionCodec = NCompression::ECodec::Lz4;

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetEnableReorder(true));
    }

private:
    NVersionedTableClient::TChunkWriterConfigPtr ChunkWriterConfig_;
    NChunkClient::TEncodingWriterOptionsPtr EncodingWriterOptions_;

    IExecutorPtr Executor_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        auto planFragment = FromProto(request->plan_fragment());
        planFragment.GetContext()->GetNodeDirectory()->MergeFrom(request->node_directory());

        auto memoryWriter = New<TMemoryWriter>();
        auto chunkWriter = CreateSchemedChunkWriter(
            ChunkWriterConfig_,
            EncodingWriterOptions_,
            memoryWriter);

        auto result = WaitFor(Executor_->Execute(planFragment, chunkWriter));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);

        response->mutable_chunk_meta()->Swap(&memoryWriter->GetChunkMeta());
        response->Attachments() = std::move(memoryWriter->GetBlocks());
        context->Reply();
    }

};

IServicePtr CreateQueryService(
    IInvokerPtr invoker,
    IExecutorPtr executor)
{
    return New<TQueryService>(
        invoker,
        executor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

