#include "stdafx.h"

#include "chunk_writer_base.h"

#include "block_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "versioned_row.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IChunkWriterPtr asyncWriter,
    // We pass key columns here in order to use TChunkWriterBase and
    // TSortedChunkWriterBase as template base interchangeably.
    const TKeyColumns& keyColumns)
    : Config_(config)
    , Options_(options)
    , KeyColumns_(keyColumns)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(Config_, Options_, asyncWriter))
{ }

TAsyncError TChunkWriterBase::Open()
{
    BlockWriter_.reset(CreateBlockWriter());
    return OKFuture;
}

TAsyncError TChunkWriterBase::Close()
{
    if (RowCount_ == 0) {
        // Empty chunk.
        return OKFuture;
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
    DataWeight_ += GetDataWeight(row);
    OnRow(row.BeginKeys(), row.EndKeys());
}

void TChunkWriterBase::OnRow(TUnversionedRow row)
{
    DataWeight_ += GetDataWeight(row);
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

    BlockMetaExt_.add_blocks()->Swap(&block.Meta);
    EncodingChunkWriter_->WriteBlock(std::move(block.Data));
}

TError TChunkWriterBase::DoClose()
{
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

TSortedChunkWriterBase::TSortedChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IChunkWriterPtr chunkWriter,
    TKeyColumns keyColumns)
    : TChunkWriterBase(config, options, chunkWriter, keyColumns)
{ }

TChunkMeta TSortedChunkWriterBase::GetMasterMeta() const
{
    auto meta = TChunkWriterBase::GetMasterMeta();
    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
    return meta;
}

TChunkMeta TSortedChunkWriterBase::GetSchedulerMeta() const
{
    return TChunkWriterBase::GetMasterMeta();
}

i64 TSortedChunkWriterBase::GetMetaSize() const
{
    return TChunkWriterBase::GetMetaSize() + BlockIndexExtSize_;
}

void TSortedChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    YCHECK(std::distance(begin, end) >= KeyColumns_.size());
    LastKey_ = TOwningKey(begin, begin + KeyColumns_.size());
    if (RowCount_ == 0) {
        ToProto(BoundaryKeysExt_.mutable_min(), LastKey_);
    }

    TChunkWriterBase::OnRow(begin, end);
}

void TSortedChunkWriterBase::OnBlockFinish()
{
    ToProto(BlockIndexExt_.add_entries(), LastKey_);
    BlockIndexExtSize_ = BlockIndexExt_.ByteSize();

    TChunkWriterBase::OnBlockFinish();
}

void TSortedChunkWriterBase::OnClose()
{
    ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);

    auto& meta = EncodingChunkWriter_->Meta();
    SetProtoExtension(meta.mutable_extensions(), BlockIndexExt_);

    TKeyColumnsExt keyColumnsExt;
    NYT::ToProto(keyColumnsExt.mutable_names(), KeyColumns_);
    SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);

    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);

    TChunkWriterBase::OnClose();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
