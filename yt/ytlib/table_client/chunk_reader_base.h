#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderBase
    : public virtual NChunkClient::IChunkReaderBase
{
public:
    TChunkReaderBase(
        NChunkClient::TSequentialReaderConfigPtr config,
        const NChunkClient::TReadLimit& lowerLimit,
        const NChunkClient::TReadLimit& upperLimit,
        NChunkClient::IChunkReaderPtr underlyingReader,
        const NChunkClient::NProto::TMiscExt& misc,
        NChunkClient::IBlockCachePtr blockCache);

    virtual TFuture<void> Open() override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

    virtual TFuture<void> GetFetchingCompletedEvent();

protected:
    const NChunkClient::TSequentialReaderConfigPtr Config_;
    const NChunkClient::TReadLimit LowerLimit_;
    const NChunkClient::TReadLimit UpperLimit_;
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::IChunkReaderPtr UnderlyingReader_;

    NChunkClient::TSequentialReaderPtr SequentialReader_;

    NChunkClient::NProto::TMiscExt Misc_;
    TFuture<void> ReadyEvent_ = VoidFuture;

    bool BlockEnded_ = false;
    bool InitFirstBlockNeeded_ = false;
    bool InitNextBlockNeeded_ = false;

    bool CheckRowLimit_ = false;
    bool CheckKeyLimit_ = false;

    TChunkedMemoryPool MemoryPool_;

    NLogging::TLogger Logger;


    bool BeginRead();
    bool OnBlockEnded();

    static int GetBlockIndexByKey(
        const TKey& key, 
        const std::vector<TOwningKey>& blockIndexKeys, 
        int beginBlockIndex = 0);

    void CheckBlockUpperLimits(const NProto::TBlockMeta& blockMeta, TNullable<int> keyColumnCount = Null);

    // These methods return min block index, satisfying the lower limit.
    int ApplyLowerRowLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyLowerKeyLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyLowerKeyLimit(const std::vector<TOwningKey>& blockIndexKeys) const;

    // These methods return max block index, satisfying the upper limit.
    int ApplyUpperRowLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyUpperKeyLimit(const NProto::TBlockMetaExt& blockMeta) const;
    int ApplyUpperKeyLimit(const std::vector<TOwningKey>& blockIndexKeys) const;

    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() = 0;

    virtual void InitFirstBlock() = 0;
    virtual void InitNextBlock() = 0;

private:
    std::vector<TUnversionedValue> WidenKey(const TOwningKey& key, int keyColumnCount);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
