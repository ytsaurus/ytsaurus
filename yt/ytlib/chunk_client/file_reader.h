#pragma once

#include "chunk_reader_allowing_repair.h"
#include "io_engine.h"

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <util/system/file.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NYT {
namespace NChunkClient {

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
        bool validateBlocksChecksums = true);

    // IReader implementation.
    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        const TReadSessionId& readSessionId,
        const std::vector<int>& blockIndexes) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        const TReadSessionId& readSessionId,
        int firstBlockIndex,
        int blockCount) override;

    virtual TFuture<NProto::TChunkMeta> GetMeta(
        const TWorkloadDescriptor& workloadDescriptor,
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        const TReadSessionId& readSessionId,
        const TNullable<int>& partitionTag = Null,
        const TNullable<std::vector<int>>& extensionTags = Null) override;

    virtual TChunkId GetChunkId() const override;

    virtual bool IsValid() const override;

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)>)
    {
        Y_UNIMPLEMENTED();
    }

private:
    const IIOEnginePtr IOEngine_;
    const TChunkId ChunkId_;
    const TString FileName_;
    const bool ValidateBlockChecksums_;

    TMutex Mutex_;
    std::atomic<bool> HasCachedDataFile_ = {false};
    TFuture<std::shared_ptr<TFileHandle>> CachedDataFile_;
    std::atomic<bool> HasCachedBlocksExt_ = {false};
    TFuture<NProto::TBlocksExt> CachedBlocksExt_;

    TFuture<std::vector<TBlock>> DoReadBlocks(
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        int firstBlockIndex,
        int blockCount,
        const TWorkloadDescriptor& workloadDescriptor);
    std::vector<TBlock> OnDataBlock(
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        int firstBlockIndex,
        int blockCount,
        const TSharedMutableRef& data);
    TFuture<NProto::TChunkMeta> DoGetMeta(
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags);
    NProto::TChunkMeta OnMetaDataBlock(
        const TString& metaFileName,
        i64 metaFileLength,
        TChunkReaderStatisticsPtr chunkDiskReadStatistis,
        const TSharedMutableRef& data);
    void DumpBrokenBlock(
        int blockIndex,
        const NProto::TBlockInfo& blockInfo,
        const TRef& block) const;
    void DumpBrokenMeta(const TRef& block) const;

    const NProto::TBlocksExt& GetBlockExts(TChunkReaderStatisticsPtr chunkDiskReadStatistis);
    const std::shared_ptr<TFileHandle>& GetDataFile();
};

DEFINE_REFCOUNTED_TYPE(TFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
