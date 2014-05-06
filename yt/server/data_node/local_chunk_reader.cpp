#include "stdafx.h"
#include "local_chunk_reader.h"

#include <core/concurrency/parallel_awaiter.h>

#include <core/tracing/trace_context.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/data_node/chunk_registry.h>
#include <server/data_node/chunk.h>
#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): consider revising
static const int FetchPriority = 0;
static const bool EnableCaching = true;

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkReader;
typedef TIntrusivePtr<TLocalChunkReader> TLocalChunkReaderPtr;

class TLocalChunkReader
    : public NChunkClient::IAsyncReader
{
public:
    TLocalChunkReader(
        TBootstrap* bootstrap,
        TChunkPtr chunk)
        : Bootstrap_(bootstrap)
        , Chunk_(chunk)
    { }

    ~TLocalChunkReader()
    {
        Chunk_->ReleaseReadLock();
    }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override
    {
        NTracing::TTraceSpanGuard guard("LocalChunkReader", "ReadBlocks");
        return New<TReadSession>(this, std::move(guard))
            ->Run(blockIndexes);
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* tags) override
    {
        NTracing::TTraceSpanGuard guard("LocalChunkReader", "GetChunkMeta");
        return Chunk_
            ->GetMeta(FetchPriority, tags)
            .Apply(BIND(
                &TLocalChunkReader::OnGotChunkMeta,
                partitionTag,
                Passed(std::move(guard))));
    }

    virtual TChunkId GetChunkId() const override
    {
        return Chunk_->GetId();
    }

private:
    TBootstrap* Bootstrap_;
    TChunkPtr Chunk_;


    class TReadSession
        : public TIntrinsicRefCounted
    {
    public:
        TReadSession(TLocalChunkReaderPtr owner, NTracing::TTraceSpanGuard guard)
            : Owner_(owner)
            , Promise_(NewPromise<TErrorOr<std::vector<TSharedRef>>>())
            , TraceSpanGuard_(std::move(guard))
        { }

        TAsyncReadResult Run(const std::vector<int>& blockIndexes)
        {
            Blocks_.resize(blockIndexes.size());

            auto blockStore = Owner_->Bootstrap_->GetBlockStore();
            auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
            for (int index = 0; index < static_cast<int>(blockIndexes.size()); ++index) {
                TBlockId blockId(Owner_->Chunk_->GetId(), blockIndexes[index]);
                awaiter->Await(
                    blockStore->GetBlock(blockId, FetchPriority, EnableCaching),
                    BIND(&TReadSession::OnBlockFetched, MakeStrong(this), index));
            }
    
            awaiter->Complete(BIND(&TReadSession::OnCompleted, MakeStrong(this)));

            return Promise_;
        }

    private:
        TLocalChunkReaderPtr Owner_;
        TPromise<TErrorOr<std::vector<TSharedRef>>> Promise_;

        NTracing::TTraceSpanGuard TraceSpanGuard_;

        std::vector<TSharedRef> Blocks_;


        void OnBlockFetched(int index, TBlockStore::TGetBlockResult result)
        {
            if (result.IsOK()) {
                Blocks_[index] = result.Value()->GetData();
            } else {
                Promise_.TrySet(TError("Error reading local chunk")
                    << result);
            }
        }

        void OnCompleted()
        {
            TraceSpanGuard_.Release();
            Promise_.TrySet(Blocks_);
        }

    };

    static TGetMetaResult OnGotChunkMeta(
        const TNullable<int>& partitionTag,
        NTracing::TTraceSpanGuard /*guard*/,
        TChunk::TGetMetaResult result)
    {
        if (!result.IsOK()) {
            return TError(result);
        }

        const auto& chunkMeta = *result.Value();
        return partitionTag
            ? TGetMetaResult(FilterChunkMetaByPartitionTag(chunkMeta, *partitionTag))
            : TGetMetaResult(chunkMeta);
    }

};

NChunkClient::IAsyncReaderPtr CreateLocalChunkReader(
    TBootstrap* bootstrap,
    const TChunkId& chunkId)
{
    auto chunkRegistry = bootstrap->GetChunkRegistry();
    auto chunk = chunkRegistry->FindChunk(chunkId);
    if (!chunk) {
        return nullptr;
    }
         
    if (!chunk->TryAcquireReadLock()) {
        return nullptr;
    }

    return New<TLocalChunkReader>(
        bootstrap,
        chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
