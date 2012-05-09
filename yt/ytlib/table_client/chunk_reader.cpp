#include "stdafx.h"
#include "chunk_reader.h"
#include "channel_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/actions/invoker.h>

#include <algorithm>
#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkReader::TKeyValidator
{
public:
    TKeyValidator(const NProto::TKey& pivot, bool leftBoundary)
        : LeftBoundary(leftBoundary)
    {
        Pivot.FromProto(pivot);
    }

    bool IsValid(const TKey& key)
    {
        int result = CompareKeys(key, Pivot);
        return LeftBoundary ? result >= 0 : result < 0;
    }

private:
    bool LeftBoundary;
    TKey Pivot;
    
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
struct TChunkReader::TIndexComparator
{
    bool operator()(const NProto::TKey& key, const NProto::TIndexRow& row)
    {
        return Comparator(CompareKeys(key, row.key()), 0);
    }

    TComparator<int> Comparator;
};

////////////////////////////////////////////////////////////////////////////////

//! Helper class aimed to asynchronously initialize the internals of TChunkReader.
class TChunkReader::TInitializer
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TInitializer> TPtr;

    TInitializer(
        TSequentialReaderConfigPtr config,
        TChunkReaderPtr chunkReader, 
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
    { }

    void Initialize()
    {
        auto chunkReader = ChunkReader.Lock();
        YASSERT(chunkReader);

        std::vector<int> tags;
        tags.push_back(GetProtoExtensionTag<NChunkHolder::NProto::TBlocks>());
        tags.push_back(GetProtoExtensionTag<NChunkHolder::NProto::TMisc>());
        tags.push_back(GetProtoExtensionTag<NProto::TChannels>());

        HasRangeRequest = (StartLimit.has_key() && (StartLimit.key().parts_size() > 0)) ||
            (EndLimit.has_key() && (EndLimit.key().parts_size() > 0));

        if (HasRangeRequest) {
            tags.push_back(GetProtoExtensionTag<NProto::TIndex>());
        }

        if (HasRangeRequest || chunkReader->Options.ReadKey) {
            tags.push_back(GetProtoExtensionTag<NProto::TKeyColumns>());
        }

        AsyncReader->AsyncGetChunkMeta(tags).Subscribe(BIND(
            &TInitializer::OnGotMeta, 
            MakeStrong(this)).Via(NChunkClient::ReaderThread->GetInvoker()));
    }

private:
    void OnFail(const TError& error, TChunkReaderPtr chunkReader) 
    {
        chunkReader->Initializer.Reset();
        chunkReader->State.Fail(error);
    }

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!result.IsOK()) {
            LOG_WARNING("Failed to download chunk meta: %s", ~result.GetMessage());
            OnFail(result, chunkReader);
            return;
        }

        // ToDo(psushin): add chunk id.
        // TODO(babenko): use TTaggedLogger
        LOG_DEBUG("Initializer got chunk meta");

        FOREACH (const auto& column, Channel.GetColumns()) {
            auto& columnInfo = chunkReader->FixedColumns[TStringBuf(column)];
            columnInfo.InChannel = true;
        }

        auto misc = GetProtoExtension<NChunkHolder::NProto::TMisc>(
            result.Value().extensions());

        StartRowIndex = 0;
        chunkReader->EndRowIndex = misc->row_count();

        if (StartLimit.has_row_index())
            StartRowIndex = std::max(StartRowIndex, StartLimit.row_index());

        if (EndLimit.has_row_index())
            chunkReader->EndRowIndex = std::min(chunkReader->EndRowIndex, EndLimit.row_index());

        if (HasRangeRequest || chunkReader->Options.ReadKey) {
            if (!misc->sorted()) {
                LOG_WARNING("Received key range read request for an unsorted chunk");
                OnFail(
                    TError("Received key range read request for an unsorted chunk"), 
                    chunkReader);
                return;
            }

            chunkReader->KeyColumns = GetProtoExtension<NProto::TKeyColumns>(
                result.Value().extensions());

            YASSERT(chunkReader->KeyColumns->values_size() > 0);
            for (int i = 0; i < chunkReader->KeyColumns->values_size(); ++i) {
                const auto& column = chunkReader->KeyColumns->values(i);
                Channel.AddColumn(column);
                auto& columnInfo = chunkReader->FixedColumns[TStringBuf(column)];
                columnInfo.KeyIndex = i;
                if (chunkReader->Channel.Contains(column))
                    columnInfo.InChannel = true;
            }

            chunkReader->CurrentKey.Reset(chunkReader->KeyColumns->values_size());
        }

        if (HasRangeRequest) {
            auto index = GetProtoExtension<NProto::TIndex>(
                result.Value().extensions());

            if (StartLimit.has_key() && StartLimit.key().parts_size() > 0) {
                StartValidator.Reset(new TKeyValidator(StartLimit.key(), true));

                typedef decltype(index->index_rows().begin()) TSampleIter;
                std::reverse_iterator<TSampleIter> rbegin(index->index_rows().end());
                std::reverse_iterator<TSampleIter> rend(index->index_rows().begin());
                auto it = std::upper_bound(
                    rbegin, 
                    rend, 
                    StartLimit.key(), 
                    TIndexComparator<std::greater>());

                if (it != rend) {
                    StartRowIndex = std::max(it->row_index() + 1, StartRowIndex);
                }
            }

            if (EndLimit.has_key() && EndLimit.key().parts_size() > 0) {
                chunkReader->EndValidator.Reset(new TKeyValidator(EndLimit.key(), false));

                auto it = std::upper_bound(
                    index->index_rows().begin(), 
                    index->index_rows().end(), 
                    EndLimit.key(), 
                    TIndexComparator<std::less>());

                if (it != index->index_rows().end()) {
                    chunkReader->EndRowIndex = std::min(
                        it->row_index(), 
                        chunkReader->EndRowIndex);
                }
            }
        }

        LOG_DEBUG("Defined row limits (StartRowIndex: %" PRId64 ", EndRowIndex: %" PRId64 ")",
            StartRowIndex,
            chunkReader->EndRowIndex);

        chunkReader->CurrentRowIndex = StartRowIndex;
        if (chunkReader->CurrentRowIndex >= chunkReader->EndRowIndex) {
            LOG_WARNING("Nothing to read from current chunk.");
            chunkReader->Initializer.Reset();
            chunkReader->State.FinishOperation();
            return;
        }

        chunkReader->Codec = GetCodec(ECodecId(misc->codec_id()));

        ProtoChannels = GetProtoExtension<NProto::TChannels>(
            result.Value().extensions());

        SelectChannels(chunkReader);
        YASSERT(SelectedChannels.size() > 0);

        LOG_DEBUG("Selected %d channels", static_cast<int>(SelectedChannels.size()));

        auto blockIndexSequence = GetBlockReadSequence(chunkReader);

        chunkReader->SequentialReader = New<TSequentialReader>(
            SequentialConfig,
            blockIndexSequence,
            AsyncReader,
            GetProtoExtension<NChunkHolder::NProto::TBlocks>(
                result.Value().extensions()));

        LOG_DEBUG("Defined block reading sequence (BlockIndexes: %s)", ~JoinToString(blockIndexSequence));

        chunkReader->ChannelReaders.reserve(SelectedChannels.size());

        chunkReader->SequentialReader->AsyncNextBlock().Subscribe(
            BIND(&TInitializer::OnFirstBlock, MakeWeak(this), 0)
            .Via(ReaderThread->GetInvoker()));
    }

    void SelectChannels(TChunkReaderPtr chunkReader)
    {
        ChunkChannels.reserve(ProtoChannels->items_size());
        for(int i = 0; i < ProtoChannels->items_size(); ++i) {
            ChunkChannels.push_back(TChannel::FromProto(ProtoChannels->items(i).channel()));
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

    bool SelectSingleChannel(TChunkReaderPtr chunkReader)
    {
        int resultIdx = -1;
        size_t minBlockCount = std::numeric_limits<size_t>::max();

        for (int i = 0; i < ChunkChannels.size(); ++i) {
            auto& channel = ChunkChannels[i];
            if (channel.Contains(Channel)) {
                size_t blockCount = ProtoChannels->items(i).blocks_size();
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
        TChunkReaderPtr chunkReader,
        std::vector<int>& result, 
        std::vector<TBlockInfo>& blockHeap) 
    {
        FOREACH (auto channelIdx, SelectedChannels) {
            const auto& protoChannel = ProtoChannels->items(channelIdx);
            int blockIndex = -1;
            i64 startRow = 0;
            i64 lastRow = 0;
            while (true) {
                ++blockIndex;
                YASSERT(blockIndex < static_cast<int>(protoChannel.blocks_size()));
                const auto& protoBlock = protoChannel.blocks(blockIndex);
                startRow = lastRow;
                lastRow += protoBlock.row_count();

                if (lastRow > StartRowIndex) {
                    blockHeap.push_back(TBlockInfo(
                        protoBlock.block_index(),
                        blockIndex,
                        channelIdx,
                        lastRow));

                    result.push_back(protoBlock.block_index());
                    StartRows.push_back(startRow);
                    break;
                }
            }
        }
    }

    std::vector<int> GetBlockReadSequence(TChunkReaderPtr chunkReader)
    {
        yvector<int> result;
        yvector<TBlockInfo> blockHeap;

        SelectOpeningBlocks(chunkReader, result, blockHeap);

        std::make_heap(blockHeap.begin(), blockHeap.end());

        while (true) {
            TBlockInfo currentBlock = blockHeap.front();
            int nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = ProtoChannels->items(currentBlock.ChannelIndex);

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            YASSERT(nextBlockIndex <= protoChannel.blocks_size());

            if (currentBlock.LastRow >= chunkReader->EndRowIndex) {
                FOREACH (auto& block, blockHeap) {
                    YASSERT(block.LastRow >= chunkReader->EndRowIndex);
                }
                break;
            }

            const auto& protoBlock = protoChannel.blocks(nextBlockIndex);

            blockHeap.push_back(TBlockInfo(
                protoBlock.block_index(),
                nextBlockIndex,
                currentBlock.ChannelIndex,
                currentBlock.LastRow + protoBlock.row_count()));

            std::push_heap(blockHeap.begin(), blockHeap.end());
            result.push_back(protoBlock.block_index());
        }

        return result;
    }

    void OnFirstBlock(int selectedChannelIndex, TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader) {
            LOG_DEBUG("Chunk reader canceled during initialization");
            return;
        }

        auto& channelIdx = SelectedChannels[selectedChannelIndex];

        LOG_DEBUG("Fetched first block for channel %d", channelIdx);

        if (!error.IsOK()) {
            LOG_WARNING("Failed to download first block in channel %d\n%s", 
                channelIdx,
                ~error.GetMessage());
            OnFail(error, chunkReader);
            return;
        }

        chunkReader->ChannelReaders.push_back(New<TChannelReader>(ChunkChannels[channelIdx]));

        auto& channelReader = chunkReader->ChannelReaders.back();
        auto compressedBlock = chunkReader->SequentialReader->GetBlock();
        auto decompressedBlock = chunkReader->Codec->Decompress(compressedBlock);
        channelReader->SetBlock(decompressedBlock);

        for (i64 rowIndex = StartRows[selectedChannelIndex]; 
            rowIndex < StartRowIndex; 
            ++rowIndex) 
        {
            YVERIFY(channelReader->NextRow());
        }

        LOG_DEBUG("Unwound rows for channel %d", channelIdx);

        ++selectedChannelIndex;
        if (selectedChannelIndex < SelectedChannels.size()) {
            chunkReader->SequentialReader->AsyncNextBlock()
                .Subscribe(BIND(
                    &TInitializer::OnFirstBlock, 
                    MakeWeak(this), 
                    selectedChannelIndex)
                .Via(ReaderThread->GetInvoker()));
        } else {
            // Create current row.
            LOG_DEBUG("All first blocks fetched");

            chunkReader->MakeCurrentRow();
            ValidateRow(TError());
        }
    }

    void ValidateRow(TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        LOG_TRACE("Validating row %" PRId64, chunkReader->CurrentRowIndex);

        YASSERT(chunkReader->CurrentRowIndex < chunkReader->EndRowIndex);
        if (~StartValidator && !StartValidator->IsValid(chunkReader->CurrentKey)) {
            // TODO(babenko): potential performance issue
            chunkReader->DoNextRow().Subscribe(
                BIND(&TInitializer::ValidateRow, MakeWeak(this))
                .Via(ReaderThread->GetInvoker()));
            return;
        }

        LOG_DEBUG("Reader initialization complete");

        // Initialization complete.
        chunkReader->Initializer.Reset();
        chunkReader->State.FinishOperation();
    }

    TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;
    TWeakPtr<TChunkReader> ChunkReader;

    TChannel Channel;

    NProto::TReadLimit StartLimit;
    NProto::TReadLimit EndLimit;

    i64 StartRowIndex;

    THolder<TKeyValidator> StartValidator;

    TAutoPtr<NProto::TChannels> ProtoChannels;
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

TChunkReader::TChunkReader(
    TSequentialReaderConfigPtr config,
    const TChannel& channel,
    NChunkClient::IAsyncReaderPtr chunkReader,
    const NProto::TReadLimit& startLimit,
    const NProto::TReadLimit& endLimit,
    const NYTree::TYson& rowAttributes,
    TOptions options)
    : Codec(NULL)
    , SequentialReader(NULL)
    , Channel(channel)
    , CurrentRowIndex(-1)
    , EndRowIndex(0)
    , Options(options)
    , RowAttributes(rowAttributes)
    , SuccessResult(MakePromise(TError()))
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(chunkReader);

    Initializer = New<TInitializer>(
        config, 
        this, 
        chunkReader, 
        startLimit, 
        endLimit);
}

TAsyncError TChunkReader::AsyncOpen()
{
    State.StartOperation();

    Initializer->Initialize();

    return State.GetOperationError();
}

TAsyncError TChunkReader::AsyncNextRow()
{
    // No thread affinity, called from SetCurrentChunk of TChunkSequenceReader.
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    State.StartOperation();

    // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
    auto asyncResult = DoNextRow();
    if (asyncResult.IsSet()) {
        OnRowFetched(asyncResult.Get());
    } else {
        asyncResult.Subscribe(BIND(&TChunkReader::OnRowFetched, MakeWeak(this)));
    }
    
    return State.GetOperationError();
}

void TChunkReader::OnRowFetched(TError error)
{
    if (error.IsOK()) {
        State.FinishOperation();
    } else {
        State.Fail(error);
    }
}

TAsyncError TChunkReader::DoNextRow()
{
    CurrentRowIndex = std::min(CurrentRowIndex + 1, EndRowIndex);

    if (CurrentRowIndex == EndRowIndex)
        return SuccessResult;

    UsedRangeColumns.clear();
    FOREACH (auto& it, FixedColumns) {
        it.second.Used = false;
    }
    CurrentRow.clear();
    CurrentKey.Reset();

    return ContinueNextRow(-1, SuccessResult, TError());
}

TAsyncError TChunkReader::ContinueNextRow(
    int channelIndex,
    TAsyncErrorPromise result,
    TError error)
{
    if (!error.IsOK()) {
        YASSERT(!result.IsSet());
        result.Set(error);
        return result;
    }

    if (channelIndex >= 0) {
        auto& channel = ChannelReaders[channelIndex];
        channel->SetBlock(Codec->Decompress(SequentialReader->GetBlock()));
    }

    ++channelIndex;

    while (channelIndex < ChannelReaders.size()) {
        auto& channel = ChannelReaders[channelIndex];
        if (!channel->NextRow()) {
            YASSERT(SequentialReader->HasNext());

            if (result.IsSet()) {
                // Possible when called directly from DoNextRow
                result = NewPromise<TError>();
            }

            SequentialReader->AsyncNextBlock().Subscribe(BIND(
                IgnoreResult(&TChunkReader::ContinueNextRow),
                MakeWeak(this),
                channelIndex,
                result));
            return result;
        }
        ++channelIndex;
    }

    MakeCurrentRow();

    if (!result.IsSet()) {
        result.Set(TError());
    }
    return result;
}

void TChunkReader::MakeCurrentRow()
{
    TLexer lexer;

    FOREACH (const auto& reader, ChannelReaders) {
        while (reader->NextColumn()) {
            auto column = reader->GetColumn();
            auto it = FixedColumns.find(column);
            if (it != FixedColumns.end()) {
                auto& columnInfo = it->second;
                if (!columnInfo.Used) {
                    columnInfo.Used = true;

                    if (columnInfo.KeyIndex >= 0) {
                        // Use first token to create key part.
                        lexer.Reset();
                        YVERIFY(lexer.Read(reader->GetValue()) > 0);
                        YASSERT(lexer.GetState() == TLexer::EState::Terminal);

                        const auto& token = lexer.GetToken();
                        switch (token.GetType()) {
                            case ETokenType::Integer:
                                CurrentKey.SetValue(columnInfo.KeyIndex, token.GetIntegerValue());
                                break;

                            case ETokenType::String:
                                CurrentKey.SetValue(columnInfo.KeyIndex, token.GetStringValue());
                                break;

                            case ETokenType::Double:
                                CurrentKey.SetValue(columnInfo.KeyIndex, token.GetDoubleValue());
                                break;

                            default:
                                CurrentKey.SetComposite(columnInfo.KeyIndex);
                                break;
                        }
                    }

                    if (columnInfo.InChannel) {
                        CurrentRow.push_back(std::make_pair(column, reader->GetValue()));
                    }
                }
            } else if (UsedRangeColumns.insert(column).second && 
                Channel.ContainsInRanges(column)) 
            {
                CurrentRow.push_back(std::make_pair(column, reader->GetValue()));
            }
        }
    }
}

TRow& TChunkReader::GetRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    return CurrentRow;
}

TKey& TChunkReader::GetKey()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    YASSERT(Options.ReadKey);

    return CurrentKey;
}

bool TChunkReader::IsValid() const
{
    if (CurrentRowIndex >= EndRowIndex)
        return false;
    if (!EndValidator)
        return true;
    return EndValidator->IsValid(CurrentKey);
}

const TYson& TChunkReader::GetRowAttributes() const
{
    return RowAttributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
