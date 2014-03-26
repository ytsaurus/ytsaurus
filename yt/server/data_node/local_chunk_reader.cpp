#include "stdafx.h"
#include "local_chunk_reader.h"

#include <core/concurrency/parallel_awaiter.h>

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
        return New<TReadSession>(this)->Run(blockIndexes);
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* tags) override
    {
        return Chunk_->GetMeta(FetchPriority, tags)
            .Apply(BIND([=] (TChunk::TGetMetaResult result) -> TGetMetaResult {
                if (!result.IsOK()) {
                    return TError(result);
                }

                const auto& chunkMeta = *result.Value();
                return partitionTag
                    ? TGetMetaResult(FilterChunkMetaByPartitionTag(chunkMeta, *partitionTag))
                    : TGetMetaResult(chunkMeta);
            }));
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
        explicit TReadSession(TLocalChunkReaderPtr owner)
            : Owner_(owner)
            , Promise_(NewPromise<TErrorOr<std::vector<TSharedRef>>>())
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
            Promise_.TrySet(Blocks_);
        }

    };

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
