#pragma once

#include "public.h"

#include "block_writer.h"
#include "chunk_meta_extensions.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/chunk_writer_base.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual NChunkClient::IChunkWriterBase
{
public:
    TChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual TFuture<void> Open() override;

    virtual TFuture<void> Close() override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

    virtual bool IsSorted() const = 0;

protected:
    TChunkWriterConfigPtr Config_;
    TChunkWriterOptionsPtr Options_;
    i64 RowCount_ = 0;

    i64 DataWeight_ = 0;

    NChunkClient::TEncodingChunkWriterPtr EncodingChunkWriter_;

    NProto::TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_ = 0;


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
        NChunkClient::IChunkWriterPtr asyncWriter,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual TFuture<void> Open() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual bool IsSorted() const override;

protected:
    TKeyColumns KeyColumns_;

    void OnRow(TUnversionedRow row);
    void OnRow(TVersionedRow row);

    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end);

    virtual void PrepareChunkMeta() override;

    virtual IBlockWriter* CreateBlockWriter() = 0;

private:
    std::unique_ptr<IBlockWriter> BlockWriter_;

    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;
    double AverageSampleSize_ = 0.0;

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
        TKeyColumns keyColumns);

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;

    virtual i64 GetMetaSize() const override;
    virtual bool IsSorted() const override;

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

} // namespace NVersionedTableClient
} // namespace NYT
