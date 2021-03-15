#pragma once

#include "block.h"
#include "io_engine.h"

#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <util/system/file.h>

#include <atomic>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IBlocksExtCache
{
    virtual ~IBlocksExtCache() = default;

    virtual TRefCountedBlocksExtPtr Find() = 0;
    virtual void Put(
        const TRefCountedChunkMetaPtr& chunkMeta,
        const TRefCountedBlocksExtPtr& blocksExt) = 0;
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

class TFileReader
    : public virtual TRefCounted
{
public:
    //! Creates a new reader.
    /*!
     *  For chunk meta version 2+, #chunkId is validated against that stored
     *  in the meta file. Passing #NullChunkId in #chunkId suppresses this check.
     */
    TFileReader(
        IIOEnginePtr ioEngine,
        TChunkId chunkId,
        TString fileName,
        bool validateBlocksChecksums = true,
        IBlocksExtCache* blocksExtCache = nullptr);

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize);

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize);

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = std::nullopt);

    //! Returns null if already prepared.
    TFuture<void> PrepareToReadChunkFragments();

    //! Reader must be prepared (see #PrepareToReadChunkFragments) prior to this call.
    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor,
        TSharedMutableRef data);

    TChunkId GetChunkId() const;

private:
    const IIOEnginePtr IOEngine_;
    const TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    IBlocksExtCache* const BlocksExtCache_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, DataFileLock_);
    TFuture<std::shared_ptr<TFileHandle>> DataFileFuture_;
    std::shared_ptr<TFileHandle> DataFile_;
    std::atomic<bool> DataFileOpened_ = false;

    TFuture<std::vector<TBlock>> DoReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        TRefCountedBlocksExtPtr blocksExt = nullptr,
        std::shared_ptr<TFileHandle> dataFile = nullptr);
    std::vector<TBlock> OnBlocksRead(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const TRefCountedBlocksExtPtr& blocksExt,
        const TSharedMutableRef& buffer);
    TFuture<TRefCountedChunkMetaPtr> DoReadMeta(
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);
    TRefCountedChunkMetaPtr OnMetaRead(
        const TString& metaFileName,
        TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedMutableRef& data);

    TFuture<TRefCountedBlocksExtPtr> ReadBlocksExt(const TClientBlockReadOptions& options);

    TFuture<std::shared_ptr<TFileHandle>> OpenDataFile();
    std::shared_ptr<TFileHandle> OnDataFileOpened(const std::shared_ptr<TFileHandle>& file);

    void DumpBrokenBlock(
        int blockIndex,
        const NProto::TBlockInfo& blockInfo,
        TRef block) const;
    void DumpBrokenMeta(TRef block) const;
};

DEFINE_REFCOUNTED_TYPE(TFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
