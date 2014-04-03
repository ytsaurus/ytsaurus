#include "stdafx.h"

#include "block_writer.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer_base_detail.h"
#include "config.h"
#include "versioned_row.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/dispatcher.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IAsyncWriterPtr asyncWriter)
    : Config_(config)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(config, options, asyncWriter))
    , RowCount_(0)
    , BlockMetaExtSize_(0)
    , SamplesExtSize_(0)
    , AverageSampleSize_(0.0)
    , DataWeight_(0)
{ }

TAsyncError TChunkWriterBase::Open()
{
    BlockWriter_.reset(CreateBlockWriter());
    return MakeFuture(TError());
}

TAsyncError TChunkWriterBase::Close()
{
    if (RowCount_ == 0) {
        // Empty chunk.
        return MakeFuture(TError());
    }

    return BIND(&TChunkWriterBase::DoClose, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TAsyncError TChunkWriterBase::GetReadyEvent()
{
    return EncodingChunkWriter_->GetReadyEvent();
}

i64 TChunkWriterBase::GetMetaSize() const
{
    // Other meta parts are negligible.
    return BlockMetaExtSize_ + SamplesExtSize_;
}

i64 TChunkWriterBase::GetDataSize() const
{
    return EncodingChunkWriter_->GetDataStatistics().compressed_data_size() +
        (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
}

TChunkMeta TChunkWriterBase::GetMasterMeta() const
{
    TChunkMeta meta;
    FillCommonMeta(&meta);
    SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
    return meta;
}

TChunkMeta TChunkWriterBase::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

TDataStatistics TChunkWriterBase::GetDataStatistics() const
{
    return EncodingChunkWriter_->GetDataStatistics();
}

void TChunkWriterBase::OnRow(TVersionedRow row)
{
    DataWeight_ += GetDataWeigth(row);
    OnRow(row.BeginKeys(), row.EndKeys());
}

void TChunkWriterBase::OnRow(TUnversionedRow row)
{
    DataWeight_ += GetDataWeigth(row);
    OnRow(row.Begin(), row.End());
}

void TChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    double avgRowSize = EncodingChunkWriter_->GetCompressionRatio() * GetUncompressedSize() / RowCount_;
    double sampleProbability = Config_->SampleRate * avgRowSize / AverageSampleSize_;

    if (RandomNumber<double>() < sampleProbability || RowCount_ == 0) {
        EmitSample(begin, end);
    }

    ++RowCount_;

    if (BlockWriter_->GetBlockSize() < Config_->BlockSize) {
        return;
    }

    FinishBlock();
}

void TChunkWriterBase::EmitSample(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    auto entry = SerializeToString(begin, end);
    SamplesExt_.add_entries(entry);
    SamplesExtSize_ += entry.length();
    AverageSampleSize_ = static_cast<double>(SamplesExtSize_) / SamplesExt_.entries_size();
}

void TChunkWriterBase::FillCommonMeta(TChunkMeta* meta) const
{
    meta->set_type(EChunkType::Table);
    meta->set_version(GetFormatVersion());
}

void TChunkWriterBase::OnBlockFinish()
{ }

void TChunkWriterBase::FinishBlock()
{
    OnBlockFinish();

    auto block = BlockWriter_->FlushBlock();
    block.Meta.set_chunk_row_count(RowCount_);

    BlockMetaExtSize_ += block.Meta.ByteSize();

    BlockMetaExt_.add_entries()->Swap(&block.Meta);
    EncodingChunkWriter_->WriteBlock(std::move(block.Data));
}

TError TChunkWriterBase::DoClose()
{
    using NYT::ToProto;

    if (BlockWriter_->GetRowCount() > 0) {
        FinishBlock();
    }

    OnClose();

    auto& meta = EncodingChunkWriter_->Meta();
    FillCommonMeta(&meta);

    SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);
    SetProtoExtension(meta.mutable_extensions(), SamplesExt_);

    auto& miscExt = EncodingChunkWriter_->MiscExt();
    miscExt.set_sorted(true);
    miscExt.set_row_count(RowCount_);
    miscExt.set_data_weight(DataWeight_);

    return EncodingChunkWriter_->Close();
}

i64 TChunkWriterBase::GetUncompressedSize() const 
{
    i64 size = EncodingChunkWriter_->GetDataStatistics().uncompressed_data_size();
    if (BlockWriter_) {
        size += BlockWriter_->GetBlockSize();
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
