#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderBase
    : public virtual NChunkClient::IChunkReaderBase
{
public:
    TChunkReaderBase(
        TChunkReaderConfigPtr config,
        const NChunkClient::TReadLimit& lowerLimit,
        const NChunkClient::TReadLimit& upperLimit,
        NChunkClient::IChunkReaderPtr underlyingReader,
        const NChunkClient::NProto::TMiscExt& misc,
        NChunkClient::IBlockCachePtr uncompressedBlockCache);

    virtual TFuture<void> Open() override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

    virtual TFuture<void> GetFetchingCompletedEvent();

protected:
    mutable NLog::TLogger Logger;

    TChunkReaderConfigPtr Config_;

    NChunkClient::TReadLimit LowerLimit_;
    NChunkClient::TReadLimit UpperLimit_;

    NChunkClient::IBlockCachePtr UncompressedBlockCache_;

    NChunkClient::IChunkReaderPtr UnderlyingReader_;
    NChunkClient::TSequentialReaderPtr SequentialReader_;

    NChunkClient::NProto::TMiscExt Misc_;
    TFuture<void> ReadyEvent_;

    bool BlockEnded_;

    TChunkedMemoryPool MemoryPool_;


    // These methods return min block index, satisfying the lower limit.
    int ApplyLowerRowLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyLowerKeyLimit(const NProto::TBlockMetaExt& blockMeta) const;

    // These methods return max block index, satisfying the upper limit.
    int ApplyUpperRowLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyUpperKeyLimit(const NProto::TBlockMetaExt& blockMeta) const;

    void DoOpen();

    void DoSwitchBlock();

    bool OnBlockEnded();

    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() = 0;

    virtual void InitFirstBlock() = 0;
    virtual void InitNextBlock() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
