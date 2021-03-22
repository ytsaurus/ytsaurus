#pragma once

#include "io_engine.h"

#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <util/system/file.h>

#include <atomic>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IBlocksExtCache
{
    virtual ~IBlocksExtCache() = default;

    virtual NChunkClient::TRefCountedBlocksExtPtr Find() = 0;
    virtual void Put(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        const NChunkClient::TRefCountedBlocksExtPtr& blocksExt) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkFragmentDescriptor
{
    //! Chunk-wise offset.
    i64 Offset;
    //! Length of the fragment.
    int Length;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkFileReader
    : public virtual TRefCounted
{
public:
    //! Creates a new reader.
    /*!
     *  For chunk meta version 2+, #chunkId is validated against that stored
     *  in the meta file. Passing #NullChunkId in #chunkId suppresses this check.
     */
    TChunkFileReader(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool validateBlocksChecksums = true,
        IBlocksExtCache* blocksExtCache = nullptr);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize);

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt);

    //! Returns null if already prepared.
    TFuture<void> PrepareToReadChunkFragments();

    //! Reader must be prepared (see #PrepareToReadChunkFragments) prior to this call.
    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor,
        TSharedMutableRef data);

    NChunkClient::TChunkId GetChunkId() const;

private:
    const IIOEnginePtr IOEngine_;
    const NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    IBlocksExtCache* const BlocksExtCache_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, DataFileLock_);
    TFuture<std::shared_ptr<TFileHandle>> DataFileFuture_;
    std::shared_ptr<TFileHandle> DataFile_;
    std::atomic<bool> DataFileOpened_ = false;

    TFuture<std::vector<NChunkClient::TBlock>> DoReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt = nullptr,
        std::shared_ptr<TFileHandle> dataFile = nullptr);
    std::vector<NChunkClient::TBlock> OnBlocksRead(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const NChunkClient::TRefCountedBlocksExtPtr& blocksExt,
        const TSharedMutableRef& buffer);
    TFuture<NChunkClient::TRefCountedChunkMetaPtr> DoReadMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);
    NChunkClient::TRefCountedChunkMetaPtr OnMetaRead(
        const TString& metaFileName,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedMutableRef& data);

    TFuture<NChunkClient::TRefCountedBlocksExtPtr> ReadBlocksExt(const NChunkClient::TClientChunkReadOptions& options);

    TFuture<std::shared_ptr<TFileHandle>> OpenDataFile();
    std::shared_ptr<TFileHandle> OnDataFileOpened(const std::shared_ptr<TFileHandle>& file);

    void DumpBrokenBlock(
        int blockIndex,
        const NChunkClient::NProto::TBlockInfo& blockInfo,
        TRef block) const;
    void DumpBrokenMeta(TRef block) const;
};

DEFINE_REFCOUNTED_TYPE(TChunkFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
