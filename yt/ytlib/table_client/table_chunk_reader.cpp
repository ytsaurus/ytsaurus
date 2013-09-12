#include "stdafx.h"
#include "config.h"
#include "table_chunk_reader.h"
#include "channel_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"

#include <core/misc/string.h>
#include <core/misc/foreach.h>
#include <core/misc/sync.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/yson/tokenizer.h>

#include <core/actions/invoker.h>

#include <core/logging/tagged_logger.h>

#include <algorithm>
#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TableReaderLogger;

////////////////////////////////////////////////////////////////////////////////

TTableChunkReaderFacade::TTableChunkReaderFacade(TTableChunkReader* reader)
    : Reader(reader)
{ }

const TRow& TTableChunkReaderFacade::GetRow() const
{
    return Reader->GetRow();
}

const NChunkClient::TNonOwningKey& TTableChunkReaderFacade::GetKey() const
{
    return Reader->GetKey();
}

const TNullable<int>& TTableChunkReaderFacade::GetTableIndex() const
{
    return Reader->GetTableIndex();
}

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
    TKeyValidator(const NChunkClient::NProto::TKey& pivot, bool leftBoundary)
        : LeftBoundary(leftBoundary)
        , Pivot(TOwningKey::FromProto(pivot))
    { }

    template <class TBuffer>
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

    inline bool operator<(const TBlockInfo& rhs) const
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

template <template <typename T> class TComparator>
struct TIndexComparator
{
    bool operator()(const NChunkClient::NProto::TKey& key, const NProto::TIndexRow& row)
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
        const NChunkClient::NProto::TReadLimit& startLimit,
        const NChunkClient::NProto::TReadLimit& endLimit)
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
        YCHECK(chunkReader);

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(AsyncReader->GetChunkId())));

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

        if (HasRangeRequest || chunkReader->Options->ReadKey) {
            tags.push_back(TProtoExtensionTag<NProto::TKeyColumnsExt>::Value);
        }

        LOG_INFO("Requesting chunk meta");

        AsyncReader->AsyncGetChunkMeta(Null, &tags).Subscribe(
            BIND(&TRegularInitializer::OnGotMeta, MakeStrong(this))
            .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

private:
    void OnFail(const TError& error, TTableChunkReaderPtr chunkReader)
    {
        LOG_WARNING(error);
        chunkReader->Initializer.Reset();
        chunkReader->ReaderState.Fail(error);
    }

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!result.IsOK()) {
            OnFail(result, chunkReader);
            return;
        }

        LOG_DEBUG("Chunk meta received");

        const auto& chunkMeta = result.GetValue();

        if (chunkMeta.type() != EChunkType::Table) {
            auto error = TError("Invalid chunk type: expected %s, actual %s",
                ~FormatEnum(EChunkType(EChunkType::Table)).Quote(),
                ~FormatEnum(EChunkType(chunkMeta.type())).Quote());
            OnFail(error, chunkReader);
            return;
        }

        if (chunkMeta.version() != FormatVersion) {
            auto error = TError("Invalid table chunk format version: expected %d, actual %d",
                FormatVersion,
                chunkMeta.version());
            OnFail(error, chunkReader);
            return;
        }

        FOREACH (const auto& column, Channel.GetColumns()) {
            auto& columnInfo = chunkReader->ColumnsMap[TStringBuf(column)];
            columnInfo.InChannel = true;
        }

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(
            chunkMeta.extensions());

        chunkReader->EndRowIndex = miscExt.row_count();

        if (StartLimit.has_row_index()) {
            chunkReader->StartRowIndex = std::max(chunkReader->StartRowIndex, StartLimit.row_index());
        }

        if (EndLimit.has_row_index()) {
            chunkReader->EndRowIndex = std::min(chunkReader->EndRowIndex, EndLimit.row_index());
        }

        if (HasRangeRequest || chunkReader->Options->ReadKey) {
            if (!miscExt.sorted()) {
                auto error = TError("Received key range read request for an unsorted chunk %s",
                    ~ToString(AsyncReader->GetChunkId()));
                OnFail(error, chunkReader);
                return;
            }

            chunkReader->KeyColumnsExt = GetProtoExtension<NProto::TKeyColumnsExt>(
                chunkMeta.extensions());

            YCHECK(chunkReader->KeyColumnsExt.values_size() > 0);
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
                chunkMeta.extensions());

            if (StartLimit.has_key() && StartLimit.key().parts_size() > 0) {
                StartValidator.reset(new TKeyValidator(StartLimit.key(), true));

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
                chunkReader->EndValidator.reset(new TKeyValidator(EndLimit.key(), false));

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
            chunkReader->IsFinished = true;
            chunkReader->ReaderState.FinishOperation();
            return;
        }

        ChannelsExt = GetProtoExtension<NProto::TChannelsExt>(chunkMeta.extensions());

        SelectChannels(chunkReader);
        YCHECK(SelectedChannels.size() > 0);

        LOG_DEBUG("Selected channels [%s]", ~JoinToString(SelectedChannels));

        auto blockSequence = GetBlockReadSequence(chunkReader);
        LOG_DEBUG("Reading %d blocks", static_cast<int>(blockSequence.size()));

        chunkReader->SequentialReader = New<TSequentialReader>(
            SequentialConfig,
            std::move(blockSequence),
            AsyncReader,
            NCompression::ECodec(miscExt.compression_codec()));

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

        // Heuristic: first try to find a channel containing the whole read channel.
        // If several exists, choose the one with the minimum number of blocks.
        if (SelectSingleChannel(chunkReader))
            return;

        auto remainder = Channel;
        for (int channelIdx = 0; channelIdx < ChunkChannels.size(); ++channelIdx) {
            const auto& currentChannel = ChunkChannels[channelIdx];
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
                YCHECK(blockIndex < static_cast<int>(protoChannel.blocks_size()));
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
            auto nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = ChannelsExt.items(currentBlock.ChannelIndex);
            auto lastRow = currentBlock.LastRow;

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            YCHECK(nextBlockIndex <= protoChannel.blocks_size());

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
        if (!chunkReader)
            return;

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
        if (chunkReader->Options->KeepBlocks)
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
            // Check end validator.
            if (!chunkReader->ValidateRow()) {
                chunkReader->Initializer.Reset();
                chunkReader->ReaderState.FinishOperation();
                return;
            }

            ValidateRow(TError());
        }
    }

    void ValidateRow(const TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!error.IsOK()) {
            OnFail(error, chunkReader);
            return;
        }

        while (true) {
            LOG_TRACE("Validating row %" PRId64, chunkReader->CurrentRowIndex);
            if (!chunkReader->GetFacade()) {
                // We have already exceed right reading limit.
                break;
            }

            YCHECK(chunkReader->CurrentRowIndex < chunkReader->EndRowIndex);
            if (~StartValidator && !StartValidator->IsValid(chunkReader->CurrentKey)) {
                // This quick check is aimed to improve potential performance issue and
                // eliminate unnecessary calls to Subscribe and BIND.
                if (!chunkReader->DoFetchNextRow()) {
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

    TChannel Channel;

    NChunkClient::NProto::TReadLimit StartLimit;
    NChunkClient::NProto::TReadLimit EndLimit;

    std::unique_ptr<TKeyValidator> StartValidator;

    NProto::TChannelsExt ChannelsExt;
    TChannels ChunkChannels;
    std::vector<int> SelectedChannels;

    //! First row of the first block in each selected channel.
    /*!
     *  Is used to set channel readers to ChunkReader's StartRow during initialization.
     */
    std::vector<i64> StartRows;
    bool HasRangeRequest;

    NLog::TTaggedLogger Logger;

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
        YCHECK(chunkReader);

        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(AsyncReader->GetChunkId())));

        std::vector<int> tags;
        tags.reserve(10);
        tags.push_back(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        tags.push_back(TProtoExtensionTag<NProto::TChannelsExt>::Value);

        LOG_INFO("Requesting chunk meta");

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
            OnFail(result, chunkReader);
            return;
        }

        LOG_INFO("Chunk meta received");

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(
            result.GetValue().extensions());
        YCHECK(miscExt.row_count() > 0);

        auto channelsExt = GetProtoExtension<NProto::TChannelsExt>(result.GetValue().extensions());

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
            chunkReader->CurrentRowIndex = chunkReader->EndRowIndex;
            chunkReader->Initializer.Reset();
            chunkReader->IsFinished = true;
            chunkReader->ReaderState.FinishOperation();
            return;
        }

        chunkReader->SequentialReader = New<TSequentialReader>(
            SequentialConfig,
            std::move(blockSequence),
            AsyncReader,
            NCompression::ECodec(miscExt.compression_codec()));

        LOG_DEBUG("Reading %d blocks for partition %d",
            static_cast<int>(blockSequence.size()),
            chunkReader->PartitionTag);

        chunkReader->ChannelReaders.push_back(New<TChannelReader>(
            TChannel::FromProto(channelsExt.items(0).channel())));

        chunkReader->DoFetchNextRow();
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

TTableChunkReader::TTableChunkReader(
    TTableChunkReaderProviderPtr provider,
    TSequentialReaderConfigPtr config,
    const TChannel& channel,
    NChunkClient::IAsyncReaderPtr chunkReader,
    const NChunkClient::NProto::TReadLimit& startLimit,
    const NChunkClient::NProto::TReadLimit& endLimit,
    TNullable<int> tableIndex,
    i64 startTableRowIndex,
    int partitionTag,
    TChunkReaderOptionsPtr options)
    : Provider(provider)
    , Facade(this)
    , IsFinished(false)
    , SequentialReader(nullptr)
    , Channel(channel)
    , Options(options)
    , TableIndex(tableIndex)
    , StartTableRowIndex(startTableRowIndex)
    , CurrentRowIndex(-1)
    , StartRowIndex(0)
    , EndRowIndex(0)
    , PartitionTag(partitionTag)
    , OnRowFetchedCallback(BIND(&TTableChunkReader::OnRowFetched, MakeWeak(this)))
    , SuccessResult(MakePromise(TError()))
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(chunkReader);

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

bool TTableChunkReader::FetchNext()
{
    YASSERT(!ReaderState.HasRunningOperation());
    YASSERT(!Initializer);
    YASSERT(!IsFinished);

    if (DoFetchNextRow()) {
        return true;
    }

    ReaderState.StartOperation();
    RowState.GetOperationError().Subscribe(OnRowFetchedCallback);
    return false;
}

void TTableChunkReader::OnRowFetched(TError error)
{
    if (error.IsOK()) {
        ReaderState.FinishOperation();
    } else {
        ReaderState.Fail(error);
    }
}

bool TTableChunkReader::DoFetchNextRow()
{
    CurrentRowIndex = std::min(CurrentRowIndex + 1, EndRowIndex);

    if (CurrentRowIndex == EndRowIndex) {
        LOG_DEBUG("Chunk reader finished");
        IsFinished = true;
        return true;
    }

    CurrentRow.clear();
    CurrentKey.Clear();

    return ContinueFetchNextRow(-1, TError());
}

bool TTableChunkReader::ContinueFetchNextRow(int channelIndex, TError error)
{
    if (!error.IsOK()) {
        YCHECK(RowState.HasRunningOperation());
        RowState.Fail(error);
        // This return value doesn't matter.
        return true;
    }

    if (channelIndex >= 0) {
        auto& channel = ChannelReaders[channelIndex];
        auto decompressedBlock = SequentialReader->GetBlock();
        if (Options->KeepBlocks)
            FetchedBlocks.push_back(decompressedBlock);
        channel->SetBlock(decompressedBlock);
    }

    ++channelIndex;

    while (channelIndex < ChannelReaders.size()) {
        auto& channel = ChannelReaders[channelIndex];
        if (!channel->NextRow()) {
            YCHECK(SequentialReader->HasNext());

            RowState.StartOperation();

            SequentialReader->AsyncNextBlock().Subscribe(BIND(
                IgnoreResult(&TTableChunkReader::ContinueFetchNextRow),
                MakeWeak(this),
                channelIndex));
            return false;
        }
        ++channelIndex;
    }

    MakeCurrentRow();

    if (ValidateRow()) {
        ++Provider->RowIndex_;
    }

    if (RowState.HasRunningOperation())
        RowState.FinishOperation();

    return true;
}

bool TTableChunkReader::ValidateRow()
{
    if ((!!EndValidator) && !EndValidator->IsValid(CurrentKey)) {
        LOG_DEBUG("Chunk reader finished");
        IsFinished = true;
        return false;
    }

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
    YASSERT(Options->ReadKey);

    return CurrentKey;
}

auto TTableChunkReader::GetFacade() const -> const TFacade*
{
    return IsFinished ? nullptr : &Facade;
}

i64 TTableChunkReader::GetTableRowIndex() const
{
    return StartTableRowIndex + CurrentRowIndex;
}

i64 TTableChunkReader::GetSessionRowCount() const
{
    return EndRowIndex - StartRowIndex;
}

NChunkClient::NProto::TDataStatistics TTableChunkReader::GetDataStatistics() const
{

    NChunkClient::NProto::TDataStatistics result;
    result.set_chunk_count(1);

    if (SequentialReader) {
        result.set_row_count(GetSessionRowIndex());
        result.set_uncompressed_data_size(SequentialReader->GetUncompressedDataSize());
        result.set_compressed_data_size(SequentialReader->GetCompressedDataSize());
    } else {
        result.set_row_count(0);
        result.set_uncompressed_data_size(0);
        result.set_compressed_data_size(0);
    }

    return result;
}

i64 TTableChunkReader::GetSessionRowIndex() const
{
    return CurrentRowIndex - StartRowIndex;
}

const TNullable<int>& TTableChunkReader::GetTableIndex() const
{
    return TableIndex;
}

TFuture<void> TTableChunkReader::GetFetchingCompleteEvent()
{
    if (SequentialReader) {
        return SequentialReader->GetFetchingCompleteEvent();
    } else {
        // Error occured during initialization.
        return MakeFuture();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableChunkReaderProvider::TTableChunkReaderProvider(
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    const NChunkClient::TSequentialReaderConfigPtr& config,
    const TChunkReaderOptionsPtr& options,
    TNullable<i64> startTableRowIndex)
    : RowIndex_(-1)
    , RowCount_(0)
    , Config(config)
    , Options(options)
    , DataStatistics(NChunkClient::NProto::ZeroDataStatistics())
{
    FOREACH (const auto& chunkSpec, chunkSpecs) {
        i64 rowCount;
        GetStatistics(chunkSpec, nullptr, &rowCount);
        RowCount_ += rowCount;
    }
}

TTableChunkReaderPtr TTableChunkReaderProvider::CreateReader(
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    const NChunkClient::IAsyncReaderPtr& chunkReader)
{
    TNullable<int> tableIndex = Null;
    if (chunkSpec.has_table_index()) {
        tableIndex = chunkSpec.table_index();
    }

    return New<TTableChunkReader>(
        this,
        Config,
        chunkSpec.has_channel() ? TChannel::FromProto(chunkSpec.channel()) : TChannel::Universal(),
        chunkReader,
        chunkSpec.start_limit(),
        chunkSpec.end_limit(),
        tableIndex,
        chunkSpec.table_row_index(),
        chunkSpec.partition_tag(),
        Options);
}

void TTableChunkReaderProvider::OnReaderOpened(
    TTableChunkReaderPtr reader,
    NChunkClient::NProto::TChunkSpec& chunkSpec)
{
    i64 rowCount;
    GetStatistics(chunkSpec, nullptr, &rowCount);
    // GetRowCount gives better estimation than original, based on meta extensions.
    RowCount_ += reader->GetSessionRowCount() - rowCount;

    TGuard<TSpinLock> guard(SpinLock);
    YCHECK(ActiveReaders.insert(reader).second);
}

void TTableChunkReaderProvider::OnReaderFinished(TTableChunkReaderPtr reader)
{
    // Number of read row may be less than expected because of key read limits.
    RowCount_ += reader->GetSessionRowIndex() - reader->GetSessionRowCount();

    TGuard<TSpinLock> guard(SpinLock);
    DataStatistics += reader->GetDataStatistics();
    YCHECK(ActiveReaders.erase(reader) == 1);
}

bool TTableChunkReaderProvider::KeepInMemory() const
{
    return Options->KeepBlocks;
}

NChunkClient::NProto::TDataStatistics TTableChunkReaderProvider::GetDataStatistics() const
{
    auto dataStatistics = DataStatistics;

    TGuard<TSpinLock> guard(SpinLock);
    FOREACH(const auto& reader, ActiveReaders) {
        dataStatistics += reader->GetDataStatistics();
    }
    return dataStatistics;
}

i64 TTableChunkReaderFacade::GetTableRowIndex() const
{
    return Reader->GetTableRowIndex();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
