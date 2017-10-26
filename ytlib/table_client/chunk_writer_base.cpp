#include "chunk_writer_base.h"
#include "private.h"
#include "block_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "name_table.h"
#include "unversioned_row.h"
#include "versioned_row.h"

#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/encoding_chunk_writer.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    // We pass key columns here in order to use TChunkWriterBase and
    // TSortedChunkWriterBase as template base interchangably.
    const TKeyColumns& keyColumns)
    : Logger(NLogging::TLogger(TableClientLogger)
        .AddTag("ChunkWriterId: %v", TGuid::Create()))
    , Config_(config)
    , Options_(options)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(
        config,
        options,
        chunkWriter,
        blockCache,
        Logger))
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

bool TChunkWriterBase::IsCloseDemanded() const
{
    return false;
}

TChunkMeta TChunkWriterBase::GetNodeMeta() const
{
    return GetMasterMeta();
}

void TChunkWriterBase::ValidateRowWeight(i64 weight)
{
    if (!Options_->ValidateRowWeight || weight < Config_->MaxRowWeight) {
        return;
    }

    THROW_ERROR_EXCEPTION("Row weight is too large")
        << TErrorAttribute("row_weight", weight)
        << TErrorAttribute("row_weight_limit", Config_->MaxRowWeight);
}

void TChunkWriterBase::ValidateColumnCount(int columnCount)
{
    if (!Options_->ValidateColumnCount || columnCount < MaxColumnId) {
        return;
    }

    THROW_ERROR_EXCEPTION("Too many columns in row")
        << TErrorAttribute("column_count", columnCount)
        << TErrorAttribute("max_column_count", MaxColumnId);
}

void TChunkWriterBase::ValidateDuplicateIds(TUnversionedRow row, const TNameTablePtr& nameTable)
{
    if (!Options_->ValidateDuplicateIds) {
        return;
    }

    auto mark = CurrentIdValidationMark_++;
    for (const auto* value = row.Begin(); value != row.End(); ++value) {
        auto id = value->Id;
        if (id >= IdValidationMarks_.size()) {
            IdValidationMarks_.resize(std::max(IdValidationMarks_.size() * 2, static_cast<size_t>(id) + 1));
        }
        auto& idMark = IdValidationMarks_[id];
        if (idMark == mark) {
            auto error = TError("Duplicate column in unversioned row")
                 << TErrorAttribute("id", id);
            if (nameTable) {
                error = error << TErrorAttribute("column_name", nameTable->GetName(id));
            }
            THROW_ERROR error;
        }
        idMark = mark;
    }
}

TDataStatistics TChunkWriterBase::GetDataStatistics() const
{
    auto dataStatistics = EncodingChunkWriter_->GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

void TChunkWriterBase::FillCommonMeta(TChunkMeta* meta) const
{
    meta->set_type(static_cast<int>(EChunkType::Table));
    meta->set_version(static_cast<int>(GetFormatVersion()));
}

void TChunkWriterBase::RegisterBlock(TBlock& block)
{
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

void TChunkWriterBase::DoClose()
{
    PrepareChunkMeta();
    EncodingChunkWriter_->Close();
}

////////////////////////////////////////////////////////////////////////////////

TSequentialChunkWriterBase::TSequentialChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    // We pass key columns here in order to use TSequentialChunkWriterBase and
    // TSortedChunkWriterBase as a template base interchangably.
    const TKeyColumns& keyColumns)
    : TChunkWriterBase(
        config,
        options,
        chunkWriter,
        blockCache)
    , KeyColumns_(keyColumns)
    , RandomGenerator_(RandomNumber<ui64>())
    , SamplingThreshold_(static_cast<ui64>(std::numeric_limits<ui64>::max() * Config_->SampleRate))
{ }

TFuture<void> TSequentialChunkWriterBase::Open()
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
    i64 weight = GetDataWeight(row);
    ValidateRowWeight(weight);
    DataWeight_ += weight;
    OnRow(row.BeginKeys(), row.EndKeys());
}

void TSequentialChunkWriterBase::OnRow(TUnversionedRow row)
{
    i64 weight = GetDataWeight(row);
    ValidateRowWeight(weight);
    ValidateColumnCount(row.GetCount());
    DataWeight_ += weight;
    OnRow(row.Begin(), row.End());
}

void TSequentialChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    if (RandomGenerator_.Generate<ui64>() < SamplingThreshold_ || RowCount_ == 0) {
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
    SmallVector<TUnversionedValue, TypicalColumnCount> sampleValues;
    for (auto it = begin; it != end; ++it) {
        sampleValues.push_back(*it);
        auto& value = sampleValues.back();
        if (value.Type == EValueType::Any) {
            // Composite types are non-comparable, so we don't store it inside samples.
            value.Length = 0;
        } 

        if (value.Type == EValueType::String) {
            value.Length = std::min(static_cast<int>(value.Length), MaxSampleSize);
        }
    }

    auto entry = SerializeToString(sampleValues.begin(), sampleValues.end());
    SamplesExt_.add_entries(entry);
    SamplesExtSize_ += entry.length();
}

void TSequentialChunkWriterBase::FinishBlock()
{
    auto block = BlockWriter_->FlushBlock();
    block.Meta.set_chunk_row_count(RowCount_);

    RegisterBlock(block);
}

void TSequentialChunkWriterBase::PrepareChunkMeta()
{
    TChunkWriterBase::PrepareChunkMeta();

    auto& meta = EncodingChunkWriter_->Meta();
    SetProtoExtension(meta.mutable_extensions(), SamplesExt_);
}

void TSequentialChunkWriterBase::DoClose()
{
    if (BlockWriter_->GetRowCount() > 0) {
        FinishBlock();
    }

    TChunkWriterBase::DoClose();
}

i64 TSequentialChunkWriterBase::GetUncompressedSize() const
{
    i64 size = EncodingChunkWriter_->GetDataStatistics().uncompressed_data_size();
    if (BlockWriter_) {
        size += BlockWriter_->GetBlockSize();
    }
    return size;
}

bool TSequentialChunkWriterBase::IsSorted() const
{
    return false;
}

bool TSequentialChunkWriterBase::IsUniqueKeys() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TSortedChunkWriterBase::TSortedChunkWriterBase(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns)
    : TSequentialChunkWriterBase(
        config,
        options,
        chunkWriter,
        blockCache,
        keyColumns)
{ }

TChunkMeta TSortedChunkWriterBase::GetMasterMeta() const
{
    auto meta = TSequentialChunkWriterBase::GetMasterMeta();
    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
    return meta;
}

i64 TSortedChunkWriterBase::GetMetaSize() const
{
    return TSequentialChunkWriterBase::GetMetaSize();
}

void TSortedChunkWriterBase::OnRow(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    // ToDo(psushin): save newKey only for last key in the block.
    YCHECK(std::distance(begin, end) >= KeyColumns_.size());
    auto newKey = TOwningKey(begin, begin + KeyColumns_.size());
    if (RowCount_ == 0) {
        ToProto(BoundaryKeysExt_.mutable_min(), newKey);
    }
    LastKey_ = std::move(newKey);

    TSequentialChunkWriterBase::OnRow(begin, end);
}

void TSortedChunkWriterBase::RegisterBlock(TBlock& block)
{
    ToProto(block.Meta.mutable_last_key(), LastKey_);
    TSequentialChunkWriterBase::RegisterBlock(block);
}

void TSortedChunkWriterBase::PrepareChunkMeta()
{
    TSequentialChunkWriterBase::PrepareChunkMeta();

    auto& miscExt = EncodingChunkWriter_->MiscExt();
    miscExt.set_sorted(true);
    miscExt.set_unique_keys(Options_->ValidateUniqueKeys);

    ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);

    auto& meta = EncodingChunkWriter_->Meta();

    TKeyColumnsExt keyColumnsExt;
    NYT::ToProto(keyColumnsExt.mutable_names(), KeyColumns_);
    SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);

    SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
}

bool TSortedChunkWriterBase::IsSorted() const
{
    return true;
}

bool TSortedChunkWriterBase::IsUniqueKeys() const
{
    return Options_->ValidateUniqueKeys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
