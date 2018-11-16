#pragma once

#include "chunk_reader_allowing_repair.h"

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <util/system/file.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NYT {
namespace NChunkClient {

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

//! Provides a local and synchronous implementation of IReader.
class TFileReader
    : public IChunkReaderAllowingRepair
{
public:
    //! Creates a new reader.
    /*!
     *  For chunk meta version 2+, #chunkId is validated against that stored
     *  in the meta file. Passing #NullChunkId in #chunkId suppresses this check.
     */
    TFileReader(
        const IIOEnginePtr& ioEngine,
        const TChunkId& chunkId,
        const TString& fileName,
        bool validateBlocksChecksums = true,
        IBlocksExtCache* blocksExtCache = nullptr);

    // IReader implementation.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes,
        const TNullable<i64>& estimatedSize) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const TNullable<i64>& estimatedSize) override;

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& options,
        TNullable<int> partitionTag = Null,
        const TNullable<std::vector<int>>& extensionTags = Null) override;

    virtual TChunkId GetChunkId() const override;

    virtual bool IsValid() const override;

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)>) override;

private:
    const IIOEnginePtr IOEngine_;
    const TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;
    IBlocksExtCache* const BlocksExtCache_;

    TMutex Mutex_;
    std::atomic<bool> HasCachedDataFile_ = {false};
    TFuture<std::shared_ptr<TFileHandle>> CachedDataFile_;
    
    TFuture<std::vector<TBlock>> DoReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        TRefCountedBlocksExtPtr blocksExt = nullptr);
    std::vector<TBlock> OnDataBlock(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const TRefCountedBlocksExtPtr& blocksExt,
        const TSharedMutableRef& data);
    TFuture<TRefCountedChunkMetaPtr> DoReadMeta(
        const TClientBlockReadOptions& options,
        TNullable<int> partitionTag,
        const TNullable<std::vector<int>>& extensionTags);
    TRefCountedChunkMetaPtr OnMetaDataBlock(
        const TString& metaFileName,
        TChunkReaderStatisticsPtr chunkReaderStatistics,
        const TSharedMutableRef& data);
    void DumpBrokenBlock(
        int blockIndex,
        const NProto::TBlockInfo& blockInfo,
        const TRef& block) const;
    void DumpBrokenMeta(const TRef& block) const;

    TFuture<TRefCountedBlocksExtPtr> ReadBlocksExt(const TClientBlockReadOptions& options);
    const std::shared_ptr<TFileHandle>& GetDataFile();
};

DEFINE_REFCOUNTED_TYPE(TFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
