#pragma once

#include "public.h"

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

    virtual TAsyncError Open() override;

    virtual TAsyncError Close() override;

    virtual TAsyncError GetReadyEvent() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

protected:
    TChunkWriterConfigPtr Config_;
    TChunkWriterOptionsPtr Options_;
    TKeyColumns KeyColumns_;
    i64 RowCount_ = 0;

    NChunkClient::TEncodingChunkWriterPtr EncodingChunkWriter_;


    void OnRow(TUnversionedRow row);
    void OnRow(TVersionedRow row);

    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end);

    virtual void OnBlockFinish();
    virtual void OnClose();

    virtual ETableChunkFormat GetFormatVersion() const = 0;
    virtual IBlockWriter* CreateBlockWriter() = 0;

private:
    std::unique_ptr<IBlockWriter> BlockWriter_;

    NProto::TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_ = 0;

    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;
    double AverageSampleSize_ = 0;

    i64 DataWeight_ = 0;


    TError DoClose();

    void EmitSample(const TUnversionedValue* begin, const TUnversionedValue* end);

    void FinishBlock();

    void FillCommonMeta(NChunkClient::NProto::TChunkMeta* meta) const;

    i64 GetUncompressedSize() const;

};

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkWriterBase
    : public TChunkWriterBase
{

public:
    TSortedChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        TKeyColumns keyColumns);

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override;

    virtual i64 GetMetaSize() const override;

protected:
    TOwningKey LastKey_;

    NProto::TBlockIndexExt BlockIndexExt_;
    i64 BlockIndexExtSize_;

    NProto::TBoundaryKeysExt BoundaryKeysExt_;


    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end) override;
    virtual void OnBlockFinish() override;
    virtual void OnClose() override;

    using TChunkWriterBase::OnRow;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
