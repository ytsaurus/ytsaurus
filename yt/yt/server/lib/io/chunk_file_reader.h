#pragma once

#include "io_engine.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <util/system/file.h>

#include <atomic>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TBlockInfo
{
    i64 Offset = 0;
    i64 Size = 0;
    ui64 Checksum = 0;
};

struct TBlocksExt final
{
    TBlocksExt() = default;

    explicit TBlocksExt(const NChunkClient::NProto::TBlocksExt& message);

    std::vector<TBlockInfo> Blocks;
    bool SyncOnClose = false;
};

DEFINE_REFCOUNTED_TYPE(TBlocksExt);

////////////////////////////////////////////////////////////////////////////////

struct IBlocksExtCache
{
    virtual ~IBlocksExtCache() = default;

    virtual TBlocksExtPtr Find() = 0;
    virtual void Put(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        const TBlocksExtPtr& blocksExt) = 0;
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
        EDirectIOPolicy useDirectIO,
        bool validateBlocksChecksums = true,
        IBlocksExtCache* blocksExtCache = nullptr);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        const std::vector<int>& blockIndexes);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount);

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt);

    //! Returns the future indicating the moment when the underlying file is
    //! open and the relevant meta is read.
    /*!
     *  Returns null if already prepared.
     *  Blocks extensions are permanently cached in the reader after this call.
     *  If file has not been opened yet, will consider #useDirectIO as a hint to use DirectIO.
     */
    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO);

    //! Reader must be prepared (see #PrepareToReadChunkFragments) prior to this call.
    IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const TChunkFragmentDescriptor& fragmentDescriptor);

    NChunkClient::TChunkId GetChunkId() const;

private:
    const IIOEnginePtr IOEngine_;
    const NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    const EDirectIOPolicy UseDirectIO_;
    IBlocksExtCache* const BlocksExtCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DataFileLock_);
    TFuture<TIOEngineHandlePtr> DataFileFuture_;
    TIOEngineHandlePtr DataFile_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ChunkFragmentReadsLock_);
    TFuture<void> ChunkFragmentReadsPreparedFuture_;
    std::atomic<bool> ChunkFragmentReadsPrepared_ = false;
    // Permanently caches blocks extension for readers with PrepareToReadChunkFragments invoked.
    NIO::TBlocksExtPtr BlocksExt_;

    TFuture<std::vector<NChunkClient::TBlock>> DoReadBlocks(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        NIO::TBlocksExtPtr blocksExt = nullptr,
        TIOEngineHandlePtr dataFile = nullptr);
    std::vector<NChunkClient::TBlock> OnBlocksRead(
        const NChunkClient::TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const NIO::TBlocksExtPtr& blocksExt,
        const IIOEngine::TReadResponse& readResponse);
    TFuture<NChunkClient::TRefCountedChunkMetaPtr> DoReadMeta(
        const NChunkClient::TClientChunkReadOptions& options,
        std::optional<int> partitionTag);
    NChunkClient::TRefCountedChunkMetaPtr OnMetaRead(
        const TString& metaFileName,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedRef& data);

    TFuture<TIOEngineHandlePtr> OpenDataFile(bool useDirectIO);
    TIOEngineHandlePtr OnDataFileOpened(const TIOEngineHandlePtr& file);

    void DumpBrokenBlock(
        int blockIndex,
        const NIO::TBlockInfo& blockInfo,
        TRef block) const;
    void DumpBrokenMeta(TRef block) const;
};

DEFINE_REFCOUNTED_TYPE(TChunkFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
