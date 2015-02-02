#include "stdafx.h"
#include "local_chunk_reader.h"

#include <core/tracing/trace_context.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/data_node/chunk.h>
#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NDataNode;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TLocalChunkReader;
typedef TIntrusivePtr<TLocalChunkReader> TLocalChunkReaderPtr;

class TLocalChunkReader
    : public NChunkClient::IChunkReader
{
public:
    TLocalChunkReader(
        TBootstrap* bootstrap,
        TReplicationReaderConfigPtr config,
        IChunkPtr chunk)
        : Bootstrap_(bootstrap)
        , Config_(config)
        , Chunk_(chunk)
    { }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        NTracing::TTraceSpanGuard guard(
            // XXX(sandello): Disable tracing due to excessive output.
            NTracing::NullTraceContext, /* NTracing::GetCurrentTraceContext(), */
            "LocalChunkReader",
            "ReadBlocks");

        auto blockStore = Bootstrap_->GetBlockStore();
        auto this_ = MakeStrong(this);

        std::vector<TFuture<TSharedRef>> asyncBlocks;
        i64 priority = 0;
        for (int blockIndex : blockIndexes) {
            auto asyncBlock =
                BIND(
                    &TBlockStore::FindBlock,
                    blockStore,
                    Chunk_->GetId(),
                    blockIndex,
                    priority,
                    Config_->EnableCaching)
                .AsyncVia(Bootstrap_->GetControlInvoker())
                .Run();

            asyncBlocks.push_back(asyncBlock.Apply(BIND([=] (const TErrorOr<TSharedRef>& blockOrError) -> TSharedRef {
                UNUSED(this_);

                if (!blockOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION(
                        NDataNode::EErrorCode::LocalChunkReaderFailed,
                        "Error reading local chunk block %v:%v",
                        Chunk_->GetId(),
                        blockIndex)
                        << blockOrError;
                }

                const auto& block = blockOrError.Value();
                if (!block) {
                    THROW_ERROR_EXCEPTION(
                        NDataNode::EErrorCode::LocalChunkReaderFailed,
                        "Local chunk block %v:%v is not available",
                        Chunk_->GetId(),
                        blockIndex);
                }

                return block;
            })));
            // Assign decreasing priorities to block requests to take advantage of sequential read.
            --priority;
        }

        return Combine(asyncBlocks);
    }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(int firstBlockIndex, int blockCount) override
    {
        // TODO(babenko): implement when first needed
        YUNIMPLEMENTED();
    }

    virtual TFuture<TChunkMeta> GetMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* extensionTags) override
    {
        NTracing::TTraceSpanGuard guard(
            // XXX(sandello): Disable tracing due to excessive output.
            NTracing::NullTraceContext, /* NTracing::GetCurrentTraceContext(), */
            "LocalChunkReader",
            "GetChunkMeta");
        return Chunk_
            ->GetMeta(0, extensionTags)
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
    const TBootstrap* Bootstrap_;
    const TReplicationReaderConfigPtr Config_;
    const IChunkPtr Chunk_;


    static TChunkMeta OnGotChunkMeta(
        const TNullable<int>& partitionTag,
        NTracing::TTraceSpanGuard /*guard*/,
        TRefCountedChunkMetaPtr meta)
    {
        return partitionTag
            ? FilterChunkMetaByPartitionTag(*meta, *partitionTag)
            : TChunkMeta(*meta);
    }

};

NChunkClient::IChunkReaderPtr CreateLocalChunkReader(
    TBootstrap* bootstrap,
    TReplicationReaderConfigPtr config,
    IChunkPtr chunk)
{
    return New<TLocalChunkReader>(
        bootstrap,
        config,
        chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
