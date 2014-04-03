#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"
#include "chunk_writer_base.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public IChunkWriterBase
{
public:
    TChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr asyncWriter);

    virtual TAsyncError Open() override;

    virtual TAsyncError Close() override;

    virtual TAsyncError GetReadyEvent() override;

    virtual i64 GetMetaSize() const override;
    virtual i64 GetDataSize() const override;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;
    virtual NProto::TBoundaryKeysExt GetBoundaryKeys() const override;

protected:
    TChunkWriterConfigPtr Config_;
    i64 RowCount_;


    void OnRow(TUnversionedRow row);
    void OnRow(TVersionedRow row);

    virtual void OnRow(const TUnversionedValue* begin, const TUnversionedValue* end);

    virtual void OnBlockFinish();
    virtual void OnClose();

    virtual ETableChunkFormat GetFormatVersion() const = 0;
    virtual IBlockWriter* CreateBlockWriter() = 0;

private:
    NChunkClient::TEncodingChunkWriterPtr EncodingChunkWriter_;

    std::unique_ptr<IBlockWriter> BlockWriter_;

    NProto::TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_;

    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_;
    double AverageSampleSize_;

    i64 DataWeight_;


    TError DoClose();

    void EmitSample(const TUnversionedValue* begin, const TUnversionedValue* end);

    void FinishBlock();

    void FillCommonMeta(NChunkClient::NProto::TChunkMeta* meta) const;

    i64 GetUncompressedSize() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
