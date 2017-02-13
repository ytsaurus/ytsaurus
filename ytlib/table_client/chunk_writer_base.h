#pragma once

#include "public.h"
#include "block_writer.h"
#include "chunk_meta_extensions.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/chunk_writer_base.h>

#include <yt/core/misc/random.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual NChunkClient::IChunkWriterBase
{
public:
    TChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual TFuture<void> Open() override;

    virtual TFuture<void> Close() override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual bool IsCloseDemanded() const override;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetNodeMeta() const override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

    virtual bool IsSorted() const = 0;
    virtual bool IsUniqueKeys() const = 0;

protected:
    const NLogging::TLogger Logger;

    const TChunkWriterConfigPtr Config_;
    const TChunkWriterOptionsPtr Options_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    const NChunkClient::TEncodingChunkWriterPtr EncodingChunkWriter_;

    NProto::TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_ = 0;

    SmallVector<i64, TypicalColumnCount> IdValidationMarks_;
    i64 CurrentIdValidationMark_ = 1;


    void ValidateRowWeight(i64 weight);
    void ValidateColumnCount(int columnCount);
    void ValidateDuplicateIds(TUnversionedRow row, const TNameTablePtr& nameTable);

    void FillCommonMeta(NChunkClient::NProto::TChunkMeta* meta) const;

    virtual void RegisterBlock(TBlock& block);

    virtual void PrepareChunkMeta();

    virtual void DoClose();

    virtual ETableChunkFormat GetFormatVersion() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Writes one block at a time.
 */
class TSequentialChunkWriterBase
    : public TChunkWriterBase
{
public:
    TSequentialChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual TFuture<void> Open() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual bool IsSorted() const override;
    virtual bool IsUniqueKeys() const override;

protected:
    const TKeyColumns KeyColumns_;

    void OnRow(TUnversionedRow row);
    void OnRow(TVersionedRow row);

    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end);

    virtual void PrepareChunkMeta() override;

    virtual IBlockWriter* CreateBlockWriter() = 0;

private:
    std::unique_ptr<IBlockWriter> BlockWriter_;

    TRandomGenerator RandomGenerator_;
    const ui64 SamplingThreshold_;
    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;

    void DoClose();

    void EmitSample(const TUnversionedValue* begin, const TUnversionedValue* end);

    void FinishBlock();

    i64 GetUncompressedSize() const;

};

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkWriterBase
    : public TSequentialChunkWriterBase
{

public:
    TSortedChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns);

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;

    virtual i64 GetMetaSize() const override;
    virtual bool IsSorted() const override;
    virtual bool IsUniqueKeys() const override;

protected:
    TOwningKey LastKey_;

    i64 BlockIndexExtSize_;

    NProto::TBoundaryKeysExt BoundaryKeysExt_;

    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end) override;
    virtual void RegisterBlock(TBlock& block) override;
    virtual void PrepareChunkMeta() override;

    using TSequentialChunkWriterBase::OnRow;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
