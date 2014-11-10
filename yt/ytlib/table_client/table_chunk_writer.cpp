#include "stdafx.h"
#include "table_chunk_writer.h"
#include "private.h"
#include "config.h"
#include "channel_writer.h"
#include "chunk_meta_extensions.h"

#include <core/misc/serialize.h>

#include <core/yson/tokenizer.h>

#include <ytlib/chunk_client/async_writer.h>
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

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TableWriterLogger;

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
void TTableChunkWriterFacade::WriteRowUnsafe(const TRow& row, const TNonOwningKey& key)
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
    NChunkClient::IAsyncWriterPtr chunkWriter,
    TOwningKey&& lastKey)
    : TChunkWriterBase(config, options, chunkWriter)
    , Facade(this)
    , Channels(options->Channels)
    , LastKey(lastKey)
    , SamplesSize(0)
    , AverageSampleSize(0)
    , IndexSize(0)
    , BasicMetaSize(0)
{
    YCHECK(config);
    YCHECK(chunkWriter);

    MiscExt.set_compression_codec(options->CompressionCodec);

    // Init trash channel.
    auto trashChannel = TChannel::Universal();
    FOREACH (const auto& channel, Channels) {
        trashChannel -= channel;
    }
    Channels.push_back(trashChannel);

    for (int i = 0; i < static_cast<int>(Channels.size()); ++i) {
        const auto& channel = Channels[i];
        *ChannelsExt.add_items()->mutable_channel() = channel.ToProto();
        auto channelWriter = New<TChannelWriter>(i, channel.GetColumns().size());
        Buffers.push_back(channelWriter);
        BuffersHeap.push_back(~channelWriter);
        CurrentBufferCapacity += channelWriter->GetCapacity();
    }

    BasicMetaSize = ChannelsExt.ByteSize() + MiscExt.ByteSize();

    if (options->KeyColumns) {
        MiscExt.set_sorted(true);
        CurrentKey.ClearAndResize(options->KeyColumns->size());
        LastKey.ClearAndResize(options->KeyColumns->size());

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
    FOREACH (const auto& writer, Buffers) {
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

    FOREACH (const auto& channel, Buffers) {
        CurrentUncompressedSize += channel->GetCurrentSize();
    }

    CurrentSize = static_cast<i64>(EncodingWriter->GetCompressionRatio() * CurrentUncompressedSize);

    while (BuffersHeap.front()->GetCurrentSize() > static_cast<size_t>(Config->BlockSize)) {
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
    FOREACH (auto& channel, columnInfo.Channels) {
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
    FOREACH (const auto& pair, row) {
        if (pair.first.length() > MaxColumnNameSize) {
            State.Fail(TError(
                "Column name %s is too long: actual size %" PRISZT ", max size %" PRISZT,
                ~Stroka(pair.first).Quote(),
                pair.first.length(),
                MaxColumnNameSize));
            return;
        }

        auto& columnInfo = GetColumnInfo(pair.first);

        if (ColumnNames.size() > MaxColumnCount) {
            State.Fail(TError(
                "Too many different columns: already found %" PRISZT ", limit %d",
                ColumnNames.size(),
                MaxColumnCount));
            return;
        }

        if (columnInfo.LastRow == RowCount) {
            if (Config->AllowDuplicateColumnNames) {
                // Ignore second and subsequent values with the same column name.
                continue;
            }
            State.Fail(TError("Duplicate column name %s",
                ~Stroka(pair.first).Quote()));
            return;
        }

        columnInfo.LastRow = RowCount;
        WriteValue(pair, columnInfo);

        if (columnInfo.KeyColumnIndex >= 0) {
            CurrentKey.SetKeyPart(columnInfo.KeyColumnIndex, pair.second, Lexer);
        }
    }

    i64 rowWeight = DataWeight - dataWeight;
    if (rowWeight > Config->MaxRowWeight) {
        State.Fail(TError("Table row is too large: current weight %" PRId64 ", max weight %" PRId64,
            rowWeight,
            Config->MaxRowWeight));
        return;
    }

    FinalizeRow(row);

    if (Options->KeyColumns) {
        if (CompareKeys(LastKey, CurrentKey) > 0) {
            State.Fail(TError(
                EErrorCode::SortOrderViolation,
                "Sort order violation (PreviousKey: %s, CurrentKey: %s)",
                ~ToString(LastKey),
                ~ToString(CurrentKey)));
            return;
        }

        LastKey = CurrentKey;
        CurrentKey.Clear();
        ProcessKey();
    }
}

// We believe that:
//  1. row doesn't contain duplicate column names.
//  2. data is sorted
// All checks are disabled.
void TTableChunkWriter::WriteRowUnsafe(const TRow& row, const TNonOwningKey& key)
{
    WriteRowUnsafe(row);
    LastKey = key;
    ProcessKey();
}

void TTableChunkWriter::WriteRowUnsafe(const TRow& row)
{
    YASSERT(State.IsActive());

    DataWeight += 1;
    FOREACH (const auto& pair, row) {
        auto& columnInfo = GetColumnInfo(pair.first);
        WriteValue(pair, columnInfo);
    }

    FinalizeRow(row);
}

void TTableChunkWriter::ProcessKey()
{
    if (RowCount == 1) {
        *BoundaryKeysExt.mutable_start() = LastKey.ToProto();
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
    FOREACH (const auto& part, blockParts) {
        size += part.Size();
    }
    blockInfo->set_block_size(size);
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

TAsyncError TTableChunkWriter::AsyncClose()
{
    YASSERT(!State.IsClosed());

    LOG_DEBUG("Closing writer (KeyColumnCount: %d)", static_cast<int>(ColumnNames.size()));

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

void TTableChunkWriter::OnFinalBlocksWritten(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    CurrentSize = EncodingWriter->GetCompressedSize();
    CurrentUncompressedSize = EncodingWriter->GetUncompressedSize();

    SetProtoExtension(Meta.mutable_extensions(), SamplesExt);

    if (Options->KeyColumns) {
        *BoundaryKeysExt.mutable_end() = LastKey.ToProto();

        const auto lastIndexRow = --IndexExt.items().end();
        if (RowCount > lastIndexRow->row_index() + 1) {
            auto* item = IndexExt.add_items();
            *item->mutable_key() = LastKey.ToProto();
            item->set_row_index(RowCount - 1);
        }

        SetProtoExtension(Meta.mutable_extensions(), IndexExt);
        SetProtoExtension(Meta.mutable_extensions(), BoundaryKeysExt);
        {
            NProto::TKeyColumnsExt keyColumnsExt;
            ToProto(keyColumnsExt.mutable_values(), Options->KeyColumns.Get());
            SetProtoExtension(Meta.mutable_extensions(), keyColumnsExt);
        }
    }

    FinalizeWriter();
}

void TTableChunkWriter::EmitIndexEntry()
{
    auto* item = IndexExt.add_items();
    *item->mutable_key() = LastKey.ToProto();
    // RowCount is already increased
    item->set_row_index(RowCount - 1);
    IndexSize += LastKey.GetSize();
}

i64 TTableChunkWriter::EmitSample(const TRow& row, NProto::TSample* sample)
{
    i64 size = sizeof(NProto::TSample);
    std::map<TStringBuf, TStringBuf> sortedRow(row.begin(), row.end());
    FOREACH (const auto& pair, sortedRow) {
        auto* part = sample->add_parts();
        part->set_column(pair.first.begin(), pair.first.size());
        // sizeof(i32) for type field.
        SamplesSize += sizeof(i32);

        NYson::TToken token;
        Lexer.GetToken(pair.second, &token);
        YCHECK(!token.IsEmpty());

        switch (token.GetType()) {
            case ETokenType::Integer:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateValue(
                    token.GetIntegerValue()).ToProto();
                size += sizeof(i64);
                break;

            case ETokenType::String: {
                auto* keyPart = part->mutable_key_part();
                keyPart->set_type(EKeyPartType::String);
                keyPart->set_str_value(token.GetStringValue().begin(), token.GetStringValue().size());
                size += token.GetStringValue().size();
                break;
            }

            case ETokenType::Double:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateValue(
                    token.GetDoubleValue()).ToProto();
                size += sizeof(double);
                break;

            default:
                *part->mutable_key_part() = TKeyPart<TStringBuf>::CreateSentinel(EKeyPartType::Composite).ToProto();
                break;
        }
    }

    return size;
}

NChunkClient::NProto::TChunkMeta TTableChunkWriter::GetMasterMeta() const
{
    YASSERT(State.IsClosed());

    static const yhash_set<int> masterMetaTags({
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TBoundaryKeysExt>::Value });

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

const NProto::TBoundaryKeysExt& TTableChunkWriter::GetBoundaryKeys() const
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

TTableChunkWriterPtr TTableChunkWriterProvider::CreateChunkWriter(NChunkClient::IAsyncWriterPtr asyncWriter)
{
    YCHECK(FinishedWriterCount == CreatedWriterCount);
    TOwningKey key;

    if (CurrentWriter) {
        key = CurrentWriter->GetLastKey();
    }

    auto writer = New<TTableChunkWriter>(
        Config,
        Options,
        asyncWriter,
        std::move(key));

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
            const auto& boundaryKeys = CurrentWriter->GetBoundaryKeys();
            *BoundaryKeysExt.mutable_start() = boundaryKeys.start();
        }
        *BoundaryKeysExt.mutable_end() = CurrentWriter->GetLastKey().ToProto();
    }
    CurrentWriter.Reset();
}

void TTableChunkWriterProvider::OnChunkClosed(TTableChunkWriterPtr writer)
{
    TGuard<TSpinLock> guard(SpinLock);
    DataStatistics += writer->GetDataStatistics();
    YCHECK(ActiveWriters.erase(writer) == 1);
}

const NProto::TBoundaryKeysExt& TTableChunkWriterProvider::GetBoundaryKeys() const
{
    return BoundaryKeysExt;
}

i64 TTableChunkWriterProvider::GetRowCount() const
{
    return GetDataStatistics().row_count();
}

const TNullable<TKeyColumns>& TTableChunkWriterProvider::GetKeyColumns() const
{
    return Options->KeyColumns;
}

NChunkClient::NProto::TDataStatistics TTableChunkWriterProvider::GetDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    auto result = DataStatistics;

    FOREACH(const auto& writer, ActiveWriters) {
        result += writer->GetDataStatistics();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
