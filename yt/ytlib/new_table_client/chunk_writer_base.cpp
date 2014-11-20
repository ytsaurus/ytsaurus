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
    // TSortedChunkWriterBase as template base interchangably.
    const TKeyColumns& keyColumns)
    : Config_(config)
    , Options_(options)
    , RowCount_(0)
    , DataWeight_(0)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(config, options, asyncWriter))
    , BlockMetaExtSize_(0)
{ }

TFuture<void> TChunkWriterBase::Open()
{
    return VoidFuture;
}

TFuture<void> TChunkWriterBase::Close()
{
    if (RowCount_ == 0) {
        // Empty chunk.
        return VoidFuture;
    }

    return BIND(&TChunkWriterBase::DoClose, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TFuture<void> TChunkWriterBase::GetReadyEvent()
{
    return EncodingChunkWriter_->GetReadyEvent();
}

i64 TChunkWriterBase::GetMetaSize() const
{
    // Other meta parts are negligible.
    return BlockMetaExtSize_;
}

i64 TChunkWriterBase::GetDataSize() const
{
    return EncodingChunkWriter_->GetDataStatistics().compressed_data_size();
}

TChunkMeta TChunkWriterBase::GetMasterMeta() const
{
    TChunkMeta meta;
    SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
    FillCommonMeta(&meta);
    return meta;
}

TChunkMeta TChunkWriterBase::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

TDataStatistics TChunkWriterBase::GetDataStatistics() const
{
    auto dataStatistics = EncodingChunkWriter_->GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

void TChunkWriterBase::FillCommonMeta(TChunkMeta* meta) const
{
    meta->set_type(EChunkType::Table);
    meta->set_version(GetFormatVersion());
}

void TChunkWriterBase::RegisterBlock(TBlock& block)
{
    block.Meta.set_chunk_row_count(RowCount_);
    block.Meta.set_block_index(BlockMetaExt_.blocks_size());

    BlockMetaExtSize_ += block.Meta.ByteSize();
    BlockMetaExt_.add_blocks()->Swap(&block.Meta);

    EncodingChunkWriter_->WriteBlock(std::move(block.Data));
}

void TChunkWriterBase::PrepareChunkMeta()
{
    auto& miscExt = EncodingChunkWriter_->MiscExt();
    miscExt.set_sorted(false);
    miscExt.set_row_count(RowCount_);
    miscExt.set_data_weight(DataWeight_);

    auto& meta = EncodingChunkWriter_->Meta();
    FillCommonMeta(&meta);

    SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);
}

TError TChunkWriterBase::DoClose()
{
    PrepareChunkMeta();

    return EncodingChunkWriter_->Close();
}

////////////////////////////////////////////////////////////////////////////////

TSequentialChunkWriterBase::TSequentialChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IChunkWriterPtr asyncWriter,
    // We pass key columns here in order to use TSequentialChunkWriterBase and
    // TSortedChunkWriterBase as a template base interchangably.
    const TKeyColumns& keyColumns)
    : TChunkWriterBase(config, options, asyncWriter)
    , KeyColumns_(keyColumns)
    , SamplesExtSize_(0)
    , AverageSampleSize_(0.0)
{ }

TAsyncError TSequentialChunkWriterBase::Open()
{
    BlockWriter_.reset(CreateBlockWriter());
    return TChunkWriterBase::Open();
}

i64 TSequentialChunkWriterBase::GetMetaSize() const
{
    // Other meta parts are negligible.
    return TChunkWriterBase::GetMetaSize() + SamplesExtSize_;
}

i64 TSequentialChunkWriterBase::GetDataSize() const
{
    return TChunkWriterBase::GetDataSize() +
        (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
}

void TSequentialChunkWriterBase::OnRow(TVersionedRow row)
{
    DataWeight_ += GetDataWeight(row);
    OnRow(row.BeginKeys(), row.EndKeys());
}

void TSequentialChunkWriterBase::OnRow(TUnversionedRow row)
{
    DataWeight_ += GetDataWeight(row);
    OnRow(row.Begin(), row.End());
}

void TSequentialChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
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

    BlockWriter_.reset(CreateBlockWriter());
}

void TSequentialChunkWriterBase::EmitSample(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    auto entry = SerializeToString(begin, end);
    SamplesExt_.add_entries(entry);
    SamplesExtSize_ += entry.length();
    AverageSampleSize_ = static_cast<double>(SamplesExtSize_) / SamplesExt_.entries_size();
}

void TSequentialChunkWriterBase::OnBlockFinish()
{ }

void TSequentialChunkWriterBase::FinishBlock()
{
    OnBlockFinish();

    auto block = BlockWriter_->FlushBlock();
    RegisterBlock(block);
}

void TSequentialChunkWriterBase::PrepareChunkMeta()
{
    TChunkWriterBase::PrepareChunkMeta();

    auto& meta = EncodingChunkWriter_->Meta();
    SetProtoExtension(meta.mutable_extensions(), SamplesExt_);
}

TError TSequentialChunkWriterBase::DoClose()
{
    if (BlockWriter_->GetRowCount() > 0) {
        FinishBlock();
    }

    return TChunkWriterBase::DoClose();
}

i64 TSequentialChunkWriterBase::GetUncompressedSize() const
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
    : TSequentialChunkWriterBase(config, options, chunkWriter, keyColumns)
{ }

TChunkMeta TSortedChunkWriterBase::GetMasterMeta() const
{
    auto meta = TSequentialChunkWriterBase::GetMasterMeta();
    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
    return meta;
}

TChunkMeta TSortedChunkWriterBase::GetSchedulerMeta() const
{
    return TSequentialChunkWriterBase::GetMasterMeta();
}

i64 TSortedChunkWriterBase::GetMetaSize() const
{
    return TSequentialChunkWriterBase::GetMetaSize() + BlockIndexExtSize_;
}

void TSortedChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    YCHECK(std::distance(begin, end) >= KeyColumns_.size());
    auto newKey = TOwningKey(begin, begin + KeyColumns_.size());
    if (RowCount_ == 0) {
        ToProto(BoundaryKeysExt_.mutable_min(), newKey);
    } else if (Options_->VerifySorted) {
        YCHECK(CompareRows(newKey, LastKey_) >= 0);
    }
    LastKey_ = std::move(newKey);

    TSequentialChunkWriterBase::OnRow(begin, end);
}

void TSortedChunkWriterBase::OnBlockFinish()
{
    ToProto(BlockIndexExt_.add_entries(), LastKey_);
    BlockIndexExtSize_ = BlockIndexExt_.ByteSize();

    TSequentialChunkWriterBase::OnBlockFinish();
}

void TSortedChunkWriterBase::PrepareChunkMeta()
{
    TSequentialChunkWriterBase::PrepareChunkMeta();

    auto& miscExt = EncodingChunkWriter_->MiscExt();
    miscExt.set_sorted(true);

    ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);

    auto& meta = EncodingChunkWriter_->Meta();
    SetProtoExtension(meta.mutable_extensions(), BlockIndexExt_);

    TKeyColumnsExt keyColumnsExt;
    NYT::ToProto(keyColumnsExt.mutable_names(), KeyColumns_);
    SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);

    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
