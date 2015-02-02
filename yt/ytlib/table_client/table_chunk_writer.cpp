#include "stdafx.h"
#include "table_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <core/misc/serialize.h>

#include <core/yson/tokenizer.h>

#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <util/random/random.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;

using NVersionedTableClient::TKey;
using NVersionedTableClient::TOwningKey;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

static const int RangeColumnIndex = -1;

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriterFacade::TTableChunkWriterFacade(TTableChunkWriter* writer)
    : Writer(writer)
{ }

void TTableChunkWriterFacade::WriteRow(const TRow& row)
{
    Writer->WriteRow(row);
}

// Used internally. All column names are guaranteed to be unique.
void TTableChunkWriterFacade::WriteRowUnsafe(const TRow& row, const TKey& key)
{
    Writer->WriteRowUnsafe(row, key);
}

void TTableChunkWriterFacade::WriteRowUnsafe(const TRow& row)
{
    Writer->WriteRowUnsafe(row);
}

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriter::TTableChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    NChunkClient::IChunkWriterPtr chunkWriter,
    TOwningKey lastKey)
    : TChunkWriterBase(config, options, chunkWriter)
    , Facade(this)
    , Channels(options->Channels)
    , CurrentKeyMemoryPool(TTableChunkWriterMemoryPoolTag())
    , LastKey(std::move(lastKey))
    , SamplesSize(0)
    , AverageSampleSize(0)
    , IndexSize(0)
    , BasicMetaSize(0)
{
    YCHECK(config);
    YCHECK(chunkWriter);

    MiscExt.set_compression_codec(static_cast<int>(options->CompressionCodec));

    // Init trash channel.
    auto trashChannel = TChannel::Universal();
    for (const auto& channel : Channels) {
        trashChannel -= channel;
    }
    Channels.push_back(trashChannel);

    for (int i = 0; i < static_cast<int>(Channels.size()); ++i) {
        const auto& channel = Channels[i];
        ToProto(ChannelsExt.add_items()->mutable_channel(), channel);
        auto channelWriter = New<TChannelWriter>(i, channel.GetColumns().size());
        Buffers.push_back(channelWriter);
        BuffersHeap.push_back(channelWriter.Get());
        CurrentBufferCapacity += channelWriter->GetCapacity();
    }

    BasicMetaSize = ChannelsExt.ByteSize() + MiscExt.ByteSize();

    if (options->KeyColumns) {
        MiscExt.set_sorted(true);

        CurrentKey = TKey::Allocate(
            &CurrentKeyMemoryPool,
            options->KeyColumns->size());

        ResetRowValues(&CurrentKey);

        for (int keyIndex = 0; keyIndex < options->KeyColumns->size(); ++keyIndex) {
            const auto& column = options->KeyColumns->at(keyIndex);
            auto& columnInfo = ColumnMap[column];
            columnInfo.KeyColumnIndex = keyIndex;

            SelectChannels(column, columnInfo);
        }
    } else {
        MiscExt.set_sorted(false);
    }
}

void TTableChunkWriter::SelectChannels(const TStringBuf& name, TColumnInfo& columnInfo)
{
    for (int channelIndex = 0; channelIndex < Channels.size(); ++channelIndex) {
        const auto& channel = Channels[channelIndex];

        for (int columnIndex = 0; columnIndex < channel.GetColumns().size(); ++columnIndex) {
            const auto& fixed = channel.GetColumns()[columnIndex];
            if (fixed == name) {
                columnInfo.Channels.push_back(TChannelColumn(Buffers[channelIndex], columnIndex));
            }
        }

        if (channel.ContainsInRanges(name)) {
            columnInfo.Channels.push_back(TChannelColumn(Buffers[channelIndex], RangeColumnIndex));
        }
    }
}

TTableChunkWriterFacade* TTableChunkWriter::GetFacade()
{
    if (State.IsActive() && EncodingWriter->IsReady()) {
        return &Facade;
    }

    return nullptr;
}

void TTableChunkWriter::FinalizeRow(const TRow& row)
{
    for (const auto& writer : Buffers) {
        auto capacity = writer->GetCapacity();
        writer->EndRow();
        CurrentBufferCapacity += writer->GetCapacity() - capacity;
    }

    if (RowCount == 0) {
        AverageSampleSize = EmitSample(row, &FirstSample);
    }

    RowCount += 1;

    double avgRowWeight = double(DataWeight) / RowCount;
    double sampleProbability = Config->SampleRate
        * avgRowWeight
        * EncodingWriter->GetCompressionRatio()
        / AverageSampleSize;

    if (RandomNumber<double>() < sampleProbability) {
        i64 maxSamplesSize = static_cast<i64>(3 *
            Config->SampleRate *
            std::max(DataWeight, Config->BlockSize) *
            EncodingWriter->GetCompressionRatio());

        if (SamplesSize < maxSamplesSize) {
            SamplesSize += EmitSample(row, SamplesExt.add_items());
            AverageSampleSize = double(SamplesSize) / SamplesExt.items_size();
        }
    }

    CurrentUncompressedSize = EncodingWriter->GetUncompressedSize();

    for (const auto& channel : Buffers) {
        CurrentUncompressedSize += channel->GetDataSize();
    }

    CurrentSize = static_cast<i64>(EncodingWriter->GetCompressionRatio() * CurrentUncompressedSize);

    while (BuffersHeap.front()->GetDataSize() > static_cast<size_t>(Config->BlockSize)) {
        PrepareBlock();
    }

    while (CurrentBufferCapacity > Config->MaxBufferSize) {
        PrepareBlock();
    }
}

auto TTableChunkWriter::GetColumnInfo(const TStringBuf& name) ->TColumnInfo&
{
    auto it = ColumnMap.find(name);
    if (it == ColumnMap.end()) {
        ColumnNames.push_back(name.ToString());
        auto& columnInfo = ColumnMap[ColumnNames.back()];
        SelectChannels(name, columnInfo);
        return columnInfo;
    }
    return it->second;
}

void TTableChunkWriter::WriteValue(const std::pair<TStringBuf, TStringBuf>& value, const TColumnInfo& columnInfo)
{
    for (auto& channel : columnInfo.Channels) {
        auto capacity = channel.Writer->GetCapacity();
        if (channel.ColumnIndex == RangeColumnIndex) {
            channel.Writer->WriteRange(value.first, value.second);
        } else {
            channel.Writer->WriteFixed(channel.ColumnIndex, value.second);
        }
        AdjustBufferHeap(channel.Writer->GetBufferIndex());
        CurrentBufferCapacity += channel.Writer->GetCapacity() - capacity;
    }

    DataWeight += value.first.size();
    DataWeight += value.second.size();
    ValueCount += 1;
}

void TTableChunkWriter::WriteRow(const TRow& row)
{
    YASSERT(State.IsActive());

    auto dataWeight = DataWeight;

    DataWeight += 1;
    for (const auto& pair : row) {
        if (pair.first.length() > MaxColumnNameSize) {
            State.Fail(TError(
                "Column name %Qv is too long: actual size %v, max size %v",
                pair.first,
                pair.first.length(),
                MaxColumnNameSize));
            return;
        }

        auto& columnInfo = GetColumnInfo(pair.first);

        if (ColumnNames.size() > MaxColumnCount) {
            State.Fail(TError(
                "Too many different columns: already found %v, limit %v",
                ColumnNames.size(),
                MaxColumnCount));
            return;
        }

        if (columnInfo.LastRow == RowCount) {
            if (Config->AllowDuplicateColumnNames) {
                // Ignore second and subsequent values with the same column name.
                continue;
            }
            State.Fail(TError("Duplicate column name %Qv",
                pair.first));
            return;
        }

        columnInfo.LastRow = RowCount;
        WriteValue(pair, columnInfo);

        if (columnInfo.KeyColumnIndex >= 0) {
            CurrentKey[columnInfo.KeyColumnIndex] = MakeKeyPart(pair.second, Lexer);
        }
    }

    i64 rowWeight = DataWeight - dataWeight;
    if (rowWeight > Config->MaxRowWeight) {
        State.Fail(TError("Table row is too large: current weight %v, max weight %v",
            rowWeight,
            Config->MaxRowWeight));
        return;
    }

    FinalizeRow(row);

    if (Options->KeyColumns) {
        if (LastKey.Get() > CurrentKey) {
            State.Fail(TError(
                EErrorCode::SortOrderViolation,
                "Sort order violation (PreviousKey: %v, CurrentKey: %v)",
                LastKey.Get(),
                CurrentKey));
            return;
        }

        LastKey = TOwningKey(CurrentKey);
        ProcessKey();
    }
}

// We believe that:
//  1. row doesn't contain duplicate column names.
//  2. data is sorted
// All checks are disabled.
void TTableChunkWriter::WriteRowUnsafe(const TRow& row, const TKey& key)
{
    WriteRowUnsafe(row);
    LastKey = TOwningKey(key);
    ProcessKey();
}

void TTableChunkWriter::WriteRowUnsafe(const TRow& row)
{
    YASSERT(State.IsActive());

    DataWeight += 1;
    for (const auto& pair : row) {
        auto& columnInfo = GetColumnInfo(pair.first);
        WriteValue(pair, columnInfo);
    }

    FinalizeRow(row);
}

void TTableChunkWriter::ProcessKey()
{
    if (RowCount == 1) {
        ToProto(BoundaryKeysExt.mutable_start(), LastKey.Get());
    }

    if (IndexSize < Config->IndexRate * DataWeight * EncodingWriter->GetCompressionRatio()) {
        EmitIndexEntry();
    }
}

void TTableChunkWriter::PrepareBlock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    PopBufferHeap();
    auto* channel = BuffersHeap.back();

    auto* blockInfo = ChannelsExt.mutable_items(channel->GetBufferIndex())->add_blocks();
    blockInfo->set_row_count(channel->GetCurrentRowCount());
    blockInfo->set_block_index(CurrentBlockIndex);

    ++CurrentBlockIndex;

    i64 size = 0;
    auto blockParts = channel->FlushBlock();
    for (const auto& part : blockParts) {
        size += part.Size();
    }
    blockInfo->set_uncompressed_size(size);
    LargestBlockSize = std::max(LargestBlockSize, size);

    CurrentBufferCapacity += channel->GetCapacity();

    EncodingWriter->WriteBlock(std::move(blockParts));
}

TTableChunkWriter::~TTableChunkWriter()
{ }

const TOwningKey& TTableChunkWriter::GetLastKey() const
{
    return LastKey;
}

TFuture<void> TTableChunkWriter::Close()
{
    YASSERT(!State.IsClosed());

    LOG_DEBUG("Closing writer (KeyColumnCount: %v)", ColumnNames.size());

    if (SamplesExt.items_size() == 0) {
        SamplesExt.add_items()->CopyFrom(FirstSample);
    }

    State.StartOperation();

    BIND(&TTableChunkWriter::FlushBlocks, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run()
        .Subscribe(
            BIND(&TTableChunkWriter::OnFinalBlocksWritten, MakeWeak(this))
                .Via(TDispatcher::Get()->GetWriterInvoker()));

    return State.GetOperationError();
}

void TTableChunkWriter::OnFinalBlocksWritten(const TError& error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    CurrentSize = EncodingWriter->GetCompressedSize();
    CurrentUncompressedSize = EncodingWriter->GetUncompressedSize();

    SetProtoExtension(Meta.mutable_extensions(), SamplesExt);

    if (Options->KeyColumns) {
        ToProto(BoundaryKeysExt.mutable_end(), LastKey.Get());

        const auto lastIndexRow = --IndexExt.items().end();
        if (RowCount > lastIndexRow->row_index() + 1) {
            auto* item = IndexExt.add_items();
            ToProto(item->mutable_key(), LastKey.Get());
            item->set_row_index(RowCount - 1);
        }

        SetProtoExtension(Meta.mutable_extensions(), IndexExt);
        SetProtoExtension(Meta.mutable_extensions(), BoundaryKeysExt);
        {
            using NYT::ToProto;

            NProto::TKeyColumnsExt keyColumnsExt;
            ToProto(keyColumnsExt.mutable_names(), *Options->KeyColumns);
            SetProtoExtension(Meta.mutable_extensions(), keyColumnsExt);
        }
    }

    FinalizeWriter();
}

void TTableChunkWriter::EmitIndexEntry()
{
    auto* item = IndexExt.add_items();
    ToProto(item->mutable_key(), LastKey.Get());
    // RowCount is already increased
    item->set_row_index(RowCount - 1);
    IndexSize += LastKey.GetCount();
}

i64 TTableChunkWriter::EmitSample(const TRow& row, NProto::TSample* sample)
{
    i64 size = sizeof(NProto::TSample);
    std::map<TStringBuf, TStringBuf> sortedRow(row.begin(), row.end());
    for (const auto& pair : sortedRow) {
        auto* part = sample->add_parts();
        part->set_column(pair.first.begin(), pair.first.size());
        // sizeof(i32) for type field.
        SamplesSize += sizeof(i32);

        NYson::TToken token;
        Lexer.GetToken(pair.second, &token);
        YCHECK(!token.IsEmpty());

        switch (token.GetType()) {
            case ETokenType::Int64: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::Int64));
                keyPart->set_int64_value(token.GetInt64Value());
                size += sizeof(i64);
                break;
            }

            case ETokenType::Uint64: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::Uint64));
                keyPart->set_uint64_value(token.GetUint64Value());
                size += sizeof(ui64);
                break;
            }

            case ETokenType::String: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::String));
                keyPart->set_str_value(token.GetStringValue().begin(), token.GetStringValue().size());
                size += token.GetStringValue().size();
                break;
            }

            case ETokenType::Double: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::Double));
                keyPart->set_double_value(token.GetDoubleValue());
                size += sizeof(double);
                break;
            }

            case ETokenType::Boolean: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::Boolean));
                keyPart->set_boolean_value(token.GetBooleanValue());
                size += 1;
                break;
            }

            default: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(static_cast<int>(EKeyPartType::Composite));
                break;
            }
        }
    }

    return size;
}

NChunkClient::NProto::TChunkMeta TTableChunkWriter::GetMasterMeta() const
{
    YASSERT(State.IsClosed());

    static const yhash_set<int> masterMetaTags({
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TOldBoundaryKeysExt>::Value });

    auto meta = Meta;
    FilterProtoExtensions(
        meta.mutable_extensions(),
        Meta.extensions(),
        masterMetaTags);

    return meta;
}

NChunkClient::NProto::TChunkMeta TTableChunkWriter::GetSchedulerMeta() const
{
    return GetMasterMeta();
}

i64 TTableChunkWriter::GetMetaSize() const
{
    return BasicMetaSize + SamplesSize + IndexSize + (CurrentBlockIndex + 1) * sizeof(NProto::TBlockInfo);
}

const NProto::TOldBoundaryKeysExt& TTableChunkWriter::GetOldBoundaryKeys() const
{
    return BoundaryKeysExt;
}

////////////////////////////////////////////////////////////////////////////////

TTableChunkWriterProvider::TTableChunkWriterProvider(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options)
    : Config(config)
    , Options(options)
    , CreatedWriterCount(0)
    , FinishedWriterCount(0)
    , DataStatistics(NChunkClient::NProto::ZeroDataStatistics())
{
    BoundaryKeysExt.mutable_start();
    BoundaryKeysExt.mutable_end();
}

TTableChunkWriterPtr TTableChunkWriterProvider::CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter)
{
    YCHECK(FinishedWriterCount == CreatedWriterCount);

    auto lastKey = CurrentWriter
        ? CurrentWriter->GetLastKey()
        : TOwningKey(EmptyKey());

    auto writer = New<TTableChunkWriter>(
        Config,
        Options,
        chunkWriter,
        std::move(lastKey));

    CurrentWriter = writer;
    ++CreatedWriterCount;

    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ActiveWriters.insert(writer).second);

    return CurrentWriter;
}

void TTableChunkWriterProvider::OnChunkFinished()
{
    ++FinishedWriterCount;
    YCHECK(FinishedWriterCount == CreatedWriterCount);

    if (Options->KeyColumns) {
        if (FinishedWriterCount == 1) {
            const auto& boundaryKeys = CurrentWriter->GetOldBoundaryKeys();
            *BoundaryKeysExt.mutable_start() = boundaryKeys.start();
        }
        ToProto(BoundaryKeysExt.mutable_end(), CurrentWriter->GetLastKey().Get());
    }
    CurrentWriter.Reset();
}

void TTableChunkWriterProvider::OnChunkClosed(TTableChunkWriterPtr writer)
{
    TGuard<TSpinLock> guard(SpinLock);
    DataStatistics += writer->GetDataStatistics();
    YCHECK(ActiveWriters.erase(writer) == 1);
}

const NProto::TOldBoundaryKeysExt& TTableChunkWriterProvider::GetOldBoundaryKeys() const
{
    return BoundaryKeysExt;
}

i64 TTableChunkWriterProvider::GetRowCount() const
{
    return GetDataStatistics().row_count();
}

NChunkClient::NProto::TDataStatistics TTableChunkWriterProvider::GetDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    auto result = DataStatistics;

    for (const auto& writer : ActiveWriters) {
        result += writer->GetDataStatistics();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
