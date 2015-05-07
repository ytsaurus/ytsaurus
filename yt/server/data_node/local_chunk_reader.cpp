#include "stdafx.h"
#include "local_chunk_reader.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <server/data_node/chunk.h>
#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static const i64 ReadPriority = 0;

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkReader;
typedef TIntrusivePtr<TLocalChunkReader> TLocalChunkReaderPtr;

class TLocalChunkReader
    : public IChunkReader
{
public:
    TLocalChunkReader(
        TBootstrap* bootstrap,
        TReplicationReaderConfigPtr config,
        IChunkPtr chunk,
        IBlockCachePtr blockCache,
        TClosure failureHandler)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Chunk_(std::move(chunk))
        , BlockCache_(std::move(blockCache))
        , FailureHandler_(std::move(failureHandler))
    { }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        auto blockStore = Bootstrap_->GetBlockStore();
        auto asyncResult = blockStore->ReadBlocks(
            Chunk_->GetId(),
            blockIndexes,
            ReadPriority,
            Config_->EnableCaching);
        return CheckReadBlocksResult(asyncResult);
    }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(int firstBlockIndex, int blockCount) override
    {
        auto blockStore = Bootstrap_->GetBlockStore();
        auto asyncResult = blockStore->ReadBlocks(
            Chunk_->GetId(),
            firstBlockIndex,
            blockCount,
            ReadPriority,
            Config_->EnableCaching);
        return CheckReadBlocksResult(asyncResult);
    }

    virtual TFuture<TChunkMeta> GetMeta(
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override
    {
        auto asyncResult = Chunk_->ReadMeta(0, extensionTags);
        return CheckGetMetaResult(asyncResult).Apply(BIND([=] (const TRefCountedChunkMetaPtr& meta) {
            return partitionTag
                ? FilterChunkMetaByPartitionTag(*meta, *partitionTag)
                : TChunkMeta(*meta);
        }));
    }

    virtual TChunkId GetChunkId() const override
    {
        return Chunk_->GetId();
    }

private:
    const TBootstrap* Bootstrap_;
    const TReplicationReaderConfigPtr Config_;
    const IChunkPtr Chunk_;
    const IBlockCachePtr BlockCache_;
    const TClosure FailureHandler_;


    void OnError(const TError& error)
    {
        if (FailureHandler_) {
            FailureHandler_.Run();
        }

        THROW_ERROR_EXCEPTION(
            NDataNode::EErrorCode::LocalChunkReaderFailed,
            "Error accessing local chunk %v",
            Chunk_->GetId())
            << error;
    }

    TFuture<std::vector<TSharedRef>> CheckReadBlocksResult(TFuture<std::vector<TSharedRef>> asyncResult)
    {
        return asyncResult.Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TSharedRef>>& result) {
            if (!result.IsOK()) {
                OnError(result);
            }

            const auto& blocks = result.Value();
            for (const auto& block : blocks) {
                if (!block) {
                    OnError(TError("Some chunk blocks are missing"));
                }
            }

            return blocks;
        }));
    }

    TFuture<TRefCountedChunkMetaPtr> CheckGetMetaResult(TFuture<TRefCountedChunkMetaPtr> asyncResult)
    {
        return asyncResult.Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TRefCountedChunkMetaPtr>& result) {
            if (!result.IsOK()) {
                OnError(result);
            }
            return result.Value();
        }));
    }

};

IChunkReaderPtr CreateLocalChunkReader(
    TBootstrap* bootstrap,
    TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    IBlockCachePtr blockCache,
    TClosure failureHandler)
{
    return New<TLocalChunkReader>(
        bootstrap,
        std::move(config),
        std::move(chunk),
        std::move(blockCache),
        std::move(failureHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
