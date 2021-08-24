#pragma once

#include "io_engine.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

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
    //! Length of the fragment.
    int Length;
    //! Chunk-wise block index.
    int BlockIndex;
    //! Block-wise offset.
    i64 BlockOffset;
};

void FormatValue(TStringBuilderBase* builder, const TChunkFragmentDescriptor& descriptor, TStringBuf spec);
TString ToString(const TChunkFragmentDescriptor& descriptor);

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
        bool useDirectIO = false,
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
        std::optional<int> partitionTag = std::nullopt);

    //! Returns the future indicating the moment when the underlying file is
    //! open and the relevant meta is read.
    /*!
     *  Returns null if already prepared.
     *  Blocks extensions is permanently cached in the reader after this call.
     */
    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options);

    //! Reader must be prepared (see #PrepareToReadChunkFragments) prior to this call.
    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor);

    NChunkClient::TChunkId GetChunkId() const;

private:
    const IIOEnginePtr IOEngine_;
    const NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    const bool UseDirectIO_;
    IBlocksExtCache* const BlocksExtCache_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, DataFileLock_);
    TFuture<TIOEngineHandlePtr> DataFileFuture_;
    TIOEngineHandlePtr DataFile_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, ChunkFragmentReadsLock_);
    TFuture<void> ChunkFragmentReadsPreparedFuture_;
    // Permanently caches blocks extension for readers with PrepareToReadChunkFragments invoked.
    NChunkClient::TRefCountedBlocksExtPtr BlocksExt_;
    std::atomic<bool> ChunkFragmentReadsPrepared_ = false;

    TFuture<std::vector<NChunkClient::TBlock>> DoReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt = nullptr,
        TIOEngineHandlePtr dataFile = nullptr);
    std::vector<NChunkClient::TBlock> OnBlocksRead(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const NChunkClient::TRefCountedBlocksExtPtr& blocksExt,
        const IIOEngine::TReadResponse& readResponse);
    TFuture<NChunkClient::TRefCountedChunkMetaPtr> DoReadMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag);
    NChunkClient::TRefCountedChunkMetaPtr OnMetaRead(
        const TString& metaFileName,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedRef& data);

    TFuture<TIOEngineHandlePtr> OpenDataFile();
    TIOEngineHandlePtr OnDataFileOpened(const TIOEngineHandlePtr& file);

    void DumpBrokenBlock(
        int blockIndex,
        const NChunkClient::NProto::TBlockInfo& blockInfo,
        TRef block) const;
    void DumpBrokenMeta(TRef block) const;
};

DEFINE_REFCOUNTED_TYPE(TChunkFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
