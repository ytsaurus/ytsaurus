#include "stdafx.h"
#include "table_chunk_reader.h"
#include "channel_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/actions/invoker.h>
#include <ytlib/logging/tagged_logger.h>

#include <algorithm>
#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableReaderLogger;

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReader::TInitializer
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

protected:
    void OnFail(const TError& error, TTableChunkReaderPtr chunkReader) 
    {
        chunkReader->Initializer.Reset();
        chunkReader->ReaderState.Fail(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReader::TKeyValidator
{
public:
    TKeyValidator(const NProto::TKey& pivot, bool leftBoundary)
        : LeftBoundary(leftBoundary)
        , Pivot(TOwningKey::FromProto(pivot))
    { }

    template<class TBuffer>
    bool IsValid(const TKey<TBuffer>& key)
    {
        int result = CompareKeys(key, Pivot);
        return LeftBoundary ? result >= 0 : result < 0;
    }

private:
    bool LeftBoundary;
    TOwningKey Pivot;

};

////////////////////////////////////////////////////////////////////////////////

//! Represents element of the heap used to determine 
//! block reading order. (see TInitializer::GetBlockReadingOrder).
struct TBlockInfo
{
    int ChunkBlockIndex;
    int ChannelBlockIndex;
    int ChannelIndex;
    i64 LastRow;

    bool operator< (const TBlockInfo& rhs)
    {
        return
            LastRow > rhs.LastRow || 
            (LastRow == rhs.LastRow && ChannelIndex > rhs.ChannelIndex);
    }

    TBlockInfo(
        int chunkBlockIndex, 
        int channelBlockIndex, 
        int channelIdx, 
        i64 lastRow)
        : ChunkBlockIndex(chunkBlockIndex)
        , ChannelBlockIndex(channelBlockIndex)
        , ChannelIndex(channelIdx)
        , LastRow(lastRow)
    { }
};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): eliminate
template <template <typename T> class TComparator>
struct TTableChunkReader::TIndexComparator
{
    bool operator()(const NProto::TKey& key, const NProto::TIndexRow& row)
    {
        return Comparator(CompareKeys(key, row.key()), 0);
    }

    TComparator<int> Comparator;
};

////////////////////////////////////////////////////////////////////////////////

//! Helper class aimed to asynchronously initialize the internals of TChunkReader.
class TTableChunkReader::TRegularInitializer
    : public TTableChunkReader::TInitializer
{
public:
    TRegularInitializer(
        TSequentialReaderConfigPtr config,
        TTableChunkReaderPtr chunkReader, 
        NChunkClient::IAsyncReaderPtr asyncReader,
        const NProto::TReadLimit& startLimit,
        const NProto::TReadLimit& endLimit)
        : SequentialConfig(config)
        , AsyncReader(asyncReader)
        , ChunkReader(chunkReader)
        , Channel(chunkReader->Channel)
        , StartLimit(startLimit)
        , EndLimit(endLimit)
        , HasRangeRequest(false)
        , Logger(TableReaderLogger)
    { }

    void Initialize()
    {
        auto chunkReader = ChunkReader.Lock();
        YASSERT(chunkReader);

        Logger.AddTag(Sprintf("ChunkId: %s", ~AsyncReader->GetChunkId().ToString()));

        std::vector<int> tags;
        tags.reserve(10);
        tags.push_back(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        tags.push_back(TProtoExtensionTag<NProto::TChannelsExt>::Value);

        HasRangeRequest =
            (StartLimit.has_key() && (StartLimit.key().parts_size() > 0)) ||
            (EndLimit.has_key() && (EndLimit.key().parts_size() > 0));

        if (HasRangeRequest) {
            tags.push_back(TProtoExtensionTag<NProto::TIndexExt>::Value);
        }

        if (HasRangeRequest || chunkReader->Options.ReadKey) {
            tags.push_back(TProtoExtensionTag<NProto::TKeyColumnsExt>::Value);
        }

        AsyncReader->AsyncGetChunkMeta(Null, &tags).Subscribe(
            BIND(&TRegularInitializer::OnGotMeta, MakeStrong(this))
            .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

private:
    void OnFail(const TError& error, TTableChunkReaderPtr chunkReader) 
    {
        chunkReader->Initializer.Reset();
        chunkReader->ReaderState.Fail(error);
    }

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!result.IsOK()) {
            auto error = TError("Failed to download chunk meta")
                << result;
            LOG_WARNING(error);
            OnFail(error, chunkReader);
            return;
        }

        LOG_DEBUG("Chunk meta received");

        if (result.Value().type() != EChunkType::Table) {
            LOG_FATAL("Invalid chunk type %d", result.Value().type());
        }

        if (result.Value().version() != FormatVersion) {
            auto error = TError("Invalid chunk format version (Expected: %d, Actual: %d)", 
                FormatVersion,
                result.Value().version());
            LOG_WARNING(error);
            OnFail(error, chunkReader);
            return;
        }

        FOREACH (const auto& column, Channel.GetColumns()) {
            auto& columnInfo = chunkReader->ColumnsMap[TStringBuf(column)];
            columnInfo.InChannel = true;
        }

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(
            result.Value().extensions());

        chunkReader->EndRowIndex = miscExt.row_count();

        if (StartLimit.has_row_index()) {
            chunkReader->StartRowIndex = std::max(chunkReader->StartRowIndex, StartLimit.row_index());
        }

        if (EndLimit.has_row_index()) {
            chunkReader->EndRowIndex = std::min(chunkReader->EndRowIndex, EndLimit.row_index());
        }

        if (HasRangeRequest || chunkReader->Options.ReadKey) {
            if (!miscExt.sorted()) {
                auto error = TError("Received key range read request for an unsorted chunk %s",
                    ~AsyncReader->GetChunkId().ToString());
                LOG_WARNING(error);
                OnFail(error, chunkReader);
                return;
            }

            chunkReader->KeyColumnsExt = GetProtoExtension<NProto::TKeyColumnsExt>(
                result.Value().extensions());

            YASSERT(chunkReader->KeyColumnsExt.values_size() > 0);
            for (int i = 0; i < chunkReader->KeyColumnsExt.values_size(); ++i) {
                const auto& column = chunkReader->KeyColumnsExt.values(i);
                Channel.AddColumn(column);
                auto& columnInfo = chunkReader->ColumnsMap[TStringBuf(column)];
                columnInfo.KeyIndex = i;
                if (chunkReader->Channel.Contains(column))
                    columnInfo.InChannel = true;
            }

            chunkReader->CurrentKey.ClearAndResize(chunkReader->KeyColumnsExt.values_size());
        }

        if (HasRangeRequest) {
            auto indexExt = GetProtoExtension<NProto::TIndexExt>(
                result.Value().extensions());

            if (StartLimit.has_key() && StartLimit.key().parts_size() > 0) {
                StartValidator.Reset(new TKeyValidator(StartLimit.key(), true));

                typedef decltype(indexExt.items().begin()) TSampleIter;
                std::reverse_iterator<TSampleIter> rbegin(indexExt.items().end());
                std::reverse_iterator<TSampleIter> rend(indexExt.items().begin());
                auto it = std::upper_bound(
                    rbegin, 
                    rend, 
                    StartLimit.key(), 
                    TIndexComparator<std::greater>());

                if (it != rend) {
                    chunkReader->StartRowIndex = std::max(it->row_index() + 1, chunkReader->StartRowIndex);
                }
            }

            if (EndLimit.has_key() && EndLimit.key().parts_size() > 0) {
                chunkReader->EndValidator.Reset(new TKeyValidator(EndLimit.key(), false));

                auto it = std::upper_bound(
                    indexExt.items().begin(), 
                    indexExt.items().end(), 
                    EndLimit.key(), 
                    TIndexComparator<std::less>());

                if (it != indexExt.items().end()) {
                    chunkReader->EndRowIndex = std::min(
                        it->row_index(), 
                        chunkReader->EndRowIndex);
                }
            }
        }

        LOG_DEBUG("Reading rows %" PRId64 "-%" PRId64,
            chunkReader->StartRowIndex,
            chunkReader->EndRowIndex);

        chunkReader->CurrentRowIndex = chunkReader->StartRowIndex;
        if (chunkReader->CurrentRowIndex >= chunkReader->EndRowIndex) {
            LOG_WARNING("Nothing to read from the current chunk");
            chunkReader->Initializer.Reset();
            chunkReader->ReaderState.FinishOperation();
            return;
        }

        ChannelsExt = GetProtoExtension<NProto::TChannelsExt>(result.Value().extensions());

        SelectChannels(chunkReader);
        YASSERT(SelectedChannels.size() > 0);

        LOG_DEBUG("Selected channels [%s]", ~JoinToString(SelectedChannels));

        auto blockSequence = GetBlockReadSequence(chunkReader);

        chunkReader->SequentialReader = New<TSequentialReader>(
            SequentialConfig,
            MoveRV(blockSequence),
            AsyncReader,
            ECodecId(miscExt.codec_id()));

        LOG_DEBUG("Reading %d blocks", static_cast<int>(blockSequence.size()));

        chunkReader->ChannelReaders.reserve(SelectedChannels.size());

        chunkReader->SequentialReader->AsyncNextBlock().Subscribe(
            BIND(&TRegularInitializer::OnStartingBlockReceived, MakeWeak(this), 0)
            .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void SelectChannels(TTableChunkReaderPtr chunkReader)
    {
        ChunkChannels.reserve(ChannelsExt.items_size());
        for (int i = 0; i < ChannelsExt.items_size(); ++i) {
            ChunkChannels.push_back(TChannel::FromProto(ChannelsExt.items(i).channel()));
        }

        // Heuristic: first try to find a channel that contain the whole read channel.
        // If several exists, choose the one with the minimum number of blocks.
        if (SelectSingleChannel(chunkReader))
            return;

        auto remainder = Channel;
        for (int channelIdx = 0; channelIdx < ChunkChannels.size(); ++channelIdx) {
            auto& currentChannel = ChunkChannels[channelIdx];
            if (currentChannel.Overlaps(remainder)) {
                remainder -= currentChannel;
                SelectedChannels.push_back(channelIdx);
                if (remainder.IsEmpty()) {
                    break;
                }
            }
        }
    }

    bool SelectSingleChannel(TTableChunkReaderPtr chunkReader)
    {
        int resultIdx = -1;
        size_t minBlockCount = std::numeric_limits<size_t>::max();

        for (int i = 0; i < ChunkChannels.size(); ++i) {
            auto& channel = ChunkChannels[i];
            if (channel.Contains(Channel)) {
                size_t blockCount = ChannelsExt.items(i).blocks_size();
                if (minBlockCount > blockCount) {
                    resultIdx = i;
                    minBlockCount = blockCount;
                }
            }
        }

        if (resultIdx < 0)
            return false;

        SelectedChannels.push_back(resultIdx); 
        return true;
    }

    void SelectOpeningBlocks(
        TTableChunkReaderPtr chunkReader,
        std::vector<TSequentialReader::TBlockInfo>& result, 
        std::vector<TBlockInfo>& blockHeap) 
    {
        FOREACH (auto channelIdx, SelectedChannels) {
            const auto& protoChannel = ChannelsExt.items(channelIdx);
            int blockIndex = -1;
            i64 startRow = 0;
            i64 lastRow = 0;
            while (true) {
                ++blockIndex;
                YASSERT(blockIndex < static_cast<int>(protoChannel.blocks_size()));
                const auto& protoBlock = protoChannel.blocks(blockIndex);

                startRow = lastRow;
                lastRow += protoBlock.row_count();

                if (lastRow > chunkReader->StartRowIndex) {
                    blockHeap.push_back(TBlockInfo(
                        protoBlock.block_index(),
                        blockIndex,
                        channelIdx,
                        lastRow));

                    result.push_back(TSequentialReader::TBlockInfo(
                        protoBlock.block_index(), 
                        protoBlock.block_size()));
                    StartRows.push_back(startRow);
                    break;
                }
            }
        }
    }

    std::vector<TSequentialReader::TBlockInfo> GetBlockReadSequence(TTableChunkReaderPtr chunkReader)
    {
        std::vector<TSequentialReader::TBlockInfo> result;
        std::vector<TBlockInfo> blockHeap;

        SelectOpeningBlocks(chunkReader, result, blockHeap);

        std::make_heap(blockHeap.begin(), blockHeap.end());

        while (true) {
            auto currentBlock = blockHeap.front();
            int nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = ChannelsExt.items(currentBlock.ChannelIndex);
            int lastRow = currentBlock.LastRow;

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            YASSERT(nextBlockIndex <= protoChannel.blocks_size());

            if (currentBlock.LastRow >= chunkReader->EndRowIndex) {
                FOREACH (auto& block, blockHeap) {
                    YASSERT(block.LastRow >= chunkReader->EndRowIndex);
                }
                break;
            }

            while (nextBlockIndex < protoChannel.blocks_size()) {
                const auto& protoBlock = protoChannel.blocks(nextBlockIndex);
                
                lastRow += protoBlock.row_count();
                blockHeap.push_back(TBlockInfo(
                    protoBlock.block_index(),
                    nextBlockIndex,
                    currentBlock.ChannelIndex,
                    lastRow));

                std::push_heap(blockHeap.begin(), blockHeap.end());
                result.push_back(TSequentialReader::TBlockInfo(
                    protoBlock.block_index(), 
                    protoBlock.block_size()));
                break;
            }
        }

        return result;
    }

    void OnStartingBlockReceived(int selectedChannelIndex, TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader) {
            LOG_DEBUG("Chunk reader canceled during initialization");
            return;
        }

        auto& channelIdx = SelectedChannels[selectedChannelIndex];

        LOG_DEBUG("Fetched starting block for channel %d", channelIdx);

        if (!error.IsOK()) {
            auto error = TError("Failed to download starting block for channel %d", 
                channelIdx);
            OnFail(error, chunkReader);
            return;
        }

        chunkReader->ChannelReaders.push_back(New<TChannelReader>(ChunkChannels[channelIdx]));

        auto& channelReader = chunkReader->ChannelReaders.back();
        auto decompressedBlock = chunkReader->SequentialReader->GetBlock();
        if (chunkReader->Options.KeepBlocks)
            chunkReader->FetchedBlocks.push_back(decompressedBlock);
        channelReader->SetBlock(decompressedBlock);

        for (i64 rowIndex = StartRows[selectedChannelIndex]; 
            rowIndex < chunkReader->StartRowIndex; 
            ++rowIndex) 
        {
            YCHECK(channelReader->NextRow());
        }

        LOG_DEBUG("Skipped initial rows for channel %d", channelIdx);

        ++selectedChannelIndex;
        if (selectedChannelIndex < SelectedChannels.size()) {
            chunkReader->SequentialReader->AsyncNextBlock()
                .Subscribe(BIND(
                    &TRegularInitializer::OnStartingBlockReceived, 
                    MakeWeak(this), 
                    selectedChannelIndex)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
        } else {
            // Create current row.
            LOG_DEBUG("All starting blocks received");

            chunkReader->MakeCurrentRow();
            ValidateRow(TError());
        }
    }

    void ValidateRow(const TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        while (true) {
            LOG_TRACE("Validating row %" PRId64, chunkReader->CurrentRowIndex);

            YASSERT(chunkReader->CurrentRowIndex < chunkReader->EndRowIndex);
            if (~StartValidator && !StartValidator->IsValid(chunkReader->CurrentKey)) {
                // This quick check is aimed to improve potential performance issue and
                // eliminate unnecessary calls to Subscribe and BIND.
                if (!chunkReader->DoNextRow()) {
                    chunkReader->RowState.GetOperationError().Subscribe(
                        BIND(&TRegularInitializer::ValidateRow, MakeWeak(this))
                        .Via(TDispatcher::Get()->GetReaderInvoker()));
                    return;
                }
            } else {
                break;
            }
        }

        LOG_DEBUG("Reader initialized");

        // Initialization complete.
        chunkReader->Initializer.Reset();
        chunkReader->ReaderState.FinishOperation();
    }

    TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;
    TWeakPtr<TTableChunkReader> ChunkReader;

    NLog::TTaggedLogger Logger;

    TChannel Channel;

    NProto::TReadLimit StartLimit;
    NProto::TReadLimit EndLimit;

    THolder<TKeyValidator> StartValidator;

    NProto::TChannelsExt ChannelsExt;
    std::vector<TChannel> ChunkChannels;
    std::vector<int> SelectedChannels;

    //! First row of the first block in each selected channel.
    /*!
     *  Is used to set channel readers to ChunkReader's StartRow during initialization.
     */
    std::vector<i64> StartRows;
    bool HasRangeRequest;
};

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReader::TPartitionInitializer
    : public TTableChunkReader::TInitializer
{
public:
    TPartitionInitializer(
        TSequentialReaderConfigPtr config,
        TTableChunkReaderPtr chunkReader, 
        NChunkClient::IAsyncReaderPtr asyncReader)
        : SequentialConfig(config)
        , AsyncReader(asyncReader)
        , ChunkReader(chunkReader)
        , Logger(TableReaderLogger)
    { }

    void Initialize()
    {
        auto chunkReader = ChunkReader.Lock();
        YASSERT(chunkReader);

        Logger.AddTag(Sprintf("ChunkId: %s", ~AsyncReader->GetChunkId().ToString()));
        LOG_DEBUG("Initializing partition table chunk reader");

        std::vector<int> tags;
        tags.reserve(10);
        tags.push_back(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        tags.push_back(TProtoExtensionTag<NProto::TChannelsExt>::Value);

        AsyncReader->AsyncGetChunkMeta(chunkReader->PartitionTag, &tags)
            .Subscribe(
                BIND(&TPartitionInitializer::OnGotMeta, MakeStrong(this))
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!result.IsOK()) {
            auto error = TError("Failed to download chunk meta")
                << result;
            LOG_WARNING(error);
            OnFail(error, chunkReader);
            return;
        }

        LOG_DEBUG("Chunk meta received");

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(
            result.Value().extensions());

        YASSERT(miscExt.row_count() > 0);


        auto channelsExt = GetProtoExtension<NProto::TChannelsExt>(result.Value().extensions());

        YCHECK(channelsExt.items_size() == 1);

        std::vector<TSequentialReader::TBlockInfo> blockSequence;
        {
            i64 rowCount = 0;
            for (int i = 0; i < channelsExt.items(0).blocks_size(); ++i) {
                const auto& blockInfo = channelsExt.items(0).blocks(i);
                if (chunkReader->PartitionTag == blockInfo.partition_tag()) {
                    blockSequence.push_back(TSequentialReader::TBlockInfo(
                        blockInfo.block_index(), 
                        blockInfo.block_size()));

                    rowCount += blockInfo.row_count();
                }
            }

            chunkReader->EndRowIndex = rowCount;
        }

        if (blockSequence.empty()) {
            LOG_DEBUG("Nothing to read for partition %d", chunkReader->PartitionTag);
            chunkReader->Initializer.Reset();
            chunkReader->ReaderState.FinishOperation();
            return;
        }

        chunkReader->SequentialReader = New<TSequentialReader>(
            SequentialConfig,
            MoveRV(blockSequence),
            AsyncReader,
            ECodecId(miscExt.codec_id()));

        LOG_DEBUG("Reading %d blocks for partition %d", 
            static_cast<int>(blockSequence.size()),
            chunkReader->PartitionTag);

        chunkReader->ChannelReaders.push_back(New<TChannelReader>(
            TChannel::FromProto(channelsExt.items(0).channel())));

        chunkReader->DoNextRow();
        chunkReader->RowState.GetOperationError().Subscribe(BIND(
            &TTableChunkReader::OnRowFetched, 
            chunkReader));

        chunkReader->Initializer.Reset();
    }

    TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;
    TWeakPtr<TTableChunkReader> ChunkReader;
    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TTableChunkReader::TTableChunkReader(TSequentialReaderConfigPtr config,
    const TChannel& channel,
    NChunkClient::IAsyncReaderPtr chunkReader,
    const NProto::TReadLimit& startLimit,
    const NProto::TReadLimit& endLimit,
    const NYTree::TYsonString& rowAttributes,
    int partitionTag,
    TReaderOptions options)
    : SequentialReader(NULL)
    , Channel(channel)
    , CurrentRowIndex(-1)
    , PartitionTag(partitionTag)
    , StartRowIndex(0)
    , EndRowIndex(0)
    , Options(options)
    , RowAttributes(rowAttributes)
    , SuccessResult(MakePromise(TError()))
    , OnRowFetchedCallback(BIND(&TTableChunkReader::OnRowFetched, MakeWeak(this)))
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(chunkReader);

    if (PartitionTag == DefaultPartitionTag) {
        Initializer = New<TRegularInitializer>(
            config, 
            this, 
            chunkReader, 
            startLimit, 
            endLimit);
    } else {
        Initializer = New<TPartitionInitializer>(
            config, 
            this, 
            chunkReader);
    }
}

TAsyncError TTableChunkReader::AsyncOpen()
{
    ReaderState.StartOperation();

    Initializer->Initialize();

    return ReaderState.GetOperationError();
}

TAsyncError TTableChunkReader::GetReadyEvent()
{
    return ReaderState.GetOperationError();
}

bool TTableChunkReader::FetchNextItem()
{
    // No thread affinity, called from SetCurrentChunk of TChunkSequenceReader.
    YASSERT(!ReaderState.HasRunningOperation());
    YASSERT(!Initializer);

    if (!DoNextRow()) {
        ReaderState.StartOperation();
        RowState.GetOperationError().Subscribe(OnRowFetchedCallback);
        return false;
    } else {
        return true;
    }
}

void TTableChunkReader::OnRowFetched(TError error)
{
    if (error.IsOK()) {
        ReaderState.FinishOperation();
    } else {
        ReaderState.Fail(error);
    }
}

bool TTableChunkReader::DoNextRow()
{
    CurrentRowIndex = std::min(CurrentRowIndex + 1, EndRowIndex);

    if (CurrentRowIndex == EndRowIndex) {
         return true;
    }

    CurrentRow.clear();
    CurrentKey.Clear();

    return ContinueNextRow(-1, TError());
}

bool TTableChunkReader::ContinueNextRow(
    int channelIndex,
    TError error)
{
    if (!error.IsOK()) {
        YASSERT(RowState.HasRunningOperation());
        RowState.Fail(error);
        // This return value doesn't matter.
        return true;
    }

    if (channelIndex >= 0) {
        auto& channel = ChannelReaders[channelIndex];
        auto decompressedBlock = SequentialReader->GetBlock();
        if (Options.KeepBlocks)
            FetchedBlocks.push_back(decompressedBlock);
        channel->SetBlock(decompressedBlock);
    }

    ++channelIndex;

    while (channelIndex < ChannelReaders.size()) {
        auto& channel = ChannelReaders[channelIndex];
        if (!channel->NextRow()) {
            YASSERT(SequentialReader->HasNext());

            RowState.StartOperation();

            SequentialReader->AsyncNextBlock().Subscribe(BIND(
                IgnoreResult(&TTableChunkReader::ContinueNextRow),
                MakeWeak(this),
                channelIndex));
            return false;
        }
        ++channelIndex;
    }

    MakeCurrentRow();

    if (RowState.HasRunningOperation())
        RowState.FinishOperation();

    return true;
}

auto TTableChunkReader::GetColumnInfo(const TStringBuf& column) -> TColumnInfo&
{
    auto it = ColumnsMap.find(column);

    if (it == ColumnsMap.end()) {
        ColumnNames.push_back(column.ToString());
        auto& columnInfo = ColumnsMap[ColumnNames.back()];
        if (Channel.ContainsInRanges(column)) {
            columnInfo.InChannel = true;
        }
        return columnInfo;
    } else
        return it->second;
}

void TTableChunkReader::MakeCurrentRow()
{
    FOREACH (const auto& reader, ChannelReaders) {
        while (reader->NextColumn()) {
            auto column = reader->GetColumn();
            auto& columnInfo = GetColumnInfo(column);
            if (columnInfo.RowIndex < CurrentRowIndex) {
                columnInfo.RowIndex = CurrentRowIndex;

                if (columnInfo.KeyIndex >= 0) {
                    // Use first token to create key part.
                    CurrentKey.SetKeyPart(
                        columnInfo.KeyIndex,
                        reader->GetValue(),
                        Lexer);
                }

                if (columnInfo.InChannel) {
                    CurrentRow.push_back(std::make_pair(column, reader->GetValue()));
                }
            }
        }
    }
}

const TRow& TTableChunkReader::GetRow() const
{
    YASSERT(!ReaderState.HasRunningOperation());
    YASSERT(!Initializer);

    return CurrentRow;
}

const TNonOwningKey& TTableChunkReader::GetKey() const
{
    YASSERT(!ReaderState.HasRunningOperation());
    YASSERT(!Initializer);

    YASSERT(Options.ReadKey);

    return CurrentKey;
}

bool TTableChunkReader::IsValid() const
{
    if (CurrentRowIndex >= EndRowIndex)
        return false;
    if (!EndValidator)
        return true;
    return EndValidator->IsValid(CurrentKey);
}

const TYsonString& TTableChunkReader::GetRowAttributes() const
{
    return RowAttributes;
}

i64 TTableChunkReader::GetRowCount() const
{
    return EndRowIndex - StartRowIndex;
}

i64 TTableChunkReader::GetRowIndex() const
{
    return CurrentRowIndex - StartRowIndex;
}

TFuture<void> TTableChunkReader::GetFetchingCompleteEvent()
{
    return SequentialReader->GetFetchingCompleteEvent();
}

////////////////////////////////////////////////////////////////////////////////

TTableChunkReaderProvider::TTableChunkReaderProvider(const NChunkClient::TSequentialReaderConfigPtr& config,
    const TReaderOptions& options)
    : Config(config)
    , Options(options)
{ }

bool TTableChunkReaderProvider::KeepInMemory() const
{
    return Options.KeepBlocks;
}

TTableChunkReaderPtr TTableChunkReaderProvider::CreateNewReader(
    const NProto::TInputChunk& inputChunk, 
    const NChunkClient::IAsyncReaderPtr& chunkReader)
{
    const auto& slice = inputChunk.slice();
    return New<TTableChunkReader>(
        Config,
        TChannel::FromProto(inputChunk.channel()),
        chunkReader,
        slice.start_limit(),
        slice.end_limit(),
        // TODO(ignat) yson type ?
        NYTree::TYsonString(inputChunk.row_attributes()),
        inputChunk.partition_tag(),
        Options); // ToDo(psushin): pass row attributes here.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
