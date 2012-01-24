#include "stdafx.h"
#include "chunk_reader.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/actions/action_util.h>

#include <algorithm>
#include <limits>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

struct TBlockInfo
{
    int ChunkBlockIndex;
    int ChannelBlockIndex;
    int ChannelIndex;
    int LastRow;

    bool operator< (const TBlockInfo& rhs)
    {
        return
            LastRow > rhs.LastRow || 
            LastRow == rhs.LastRow && ChannelIndex > rhs.ChannelIndex;
    }

    TBlockInfo(
        int chunkBlockIndex, 
        int channelBlockIndex, 
        int channelIdx, 
        int lastRow)
        : ChunkBlockIndex(chunkBlockIndex)
        , ChannelBlockIndex(channelBlockIndex)
        , ChannelIndex(channelIdx)
        , LastRow(lastRow)
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Helper class aimed to asynchronously initialize the internals of TChunkReader.
class TChunkReader::TInitializer
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TInitializer> TPtr;

    TInitializer(
        TSequentialReader::TConfig* config,
        TChunkReader::TPtr chunkReader, 
        NChunkClient::IAsyncReader* asyncReader)
        : SequentialConfig(config)
        , AsyncReader(asyncReader)
        , ChunkReader(chunkReader)
    { }

    void Initialize()
    {
        AsyncReader->AsyncGetChunkInfo()->Subscribe(FromMethod(
            &TInitializer::OnGotMeta, 
            TPtr(this))->Via(ReaderThread->GetInvoker()));
    }

private:
    void OnGotMeta(NChunkClient::IAsyncReader::TGetInfoResult result)
    {
        if (!result.IsOK()) {
            ChunkReader->State.Fail(result);
            return;
        }

        Attributes = result.Value().attributes().GetExtension(NProto::TTableChunkAttributes::table_attributes);
        ChunkReader->Codec = GetCodec(ECodecId(Attributes.codec_id()));

        SelectChannels();
        YASSERT(SelectedChannels.size() > 0);

        yvector<int> blockIndexSequence = GetBlockReadingOrder();
        ChunkReader->SequentialReader = New<TSequentialReader>(
            ~SequentialConfig,
            blockIndexSequence,
            ~AsyncReader);

        ChunkReader->ChannelReaders.reserve(SelectedChannels.size());

        ChunkReader->SequentialReader->AsyncNextBlock()->Subscribe(FromMethod(
            &TInitializer::OnFirstBlock,
            TPtr(this),
            0)->Via(ReaderThread->GetInvoker()));
    }

    void SelectChannels()
    {
        ChunkChannels.reserve(Attributes.chunk_channels_size());
        for(int i = 0; i < Attributes.chunk_channels_size(); ++i) {
            ChunkChannels.push_back(TChannel::FromProto(Attributes.chunk_channels(i).channel()));
        }

        // Heuristic: first try to find a channel that contain the whole read channel.
        // If several exists, choose the one with the minimum number of blocks.
        if (SelectSingleChannel())
            return;

        auto remainder = ChunkReader->Channel;
        for (int channelIdx = 0; channelIdx < ChunkChannels.ysize(); ++channelIdx) {
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

    bool SelectSingleChannel()
    {
        int resultIdx = -1;
        size_t minBlockCount = std::numeric_limits<size_t>::max();

        for (int i = 0; i < Attributes.chunk_channels_size(); ++i) {
            auto& channel = ChunkChannels[i];
            if (channel.Contains(ChunkReader->Channel)) {
                size_t blockCount = Attributes.chunk_channels(i).blocks_size();
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

    void SelectOpeningBlocks(yvector<int>& result, yvector<TBlockInfo>& blockHeap) {
        FOREACH (auto channelIdx, SelectedChannels) {
            const auto& protoChannel = Attributes.chunk_channels(channelIdx);
            int blockIndex = -1;
            int startRow = 0;
            int lastRow = 0;
            do {
                ++blockIndex;
                YASSERT(blockIndex < (int)protoChannel.blocks_size());
                const auto& protoBlock = protoChannel.blocks(blockIndex);
                // When a new block is set in TChannelReader, reader is virtually 
                // one row behind its real starting row. E.g. for the first row of 
                // the channel we consider start row to be -1.
                startRow = lastRow - 1;
                lastRow += protoBlock.row_count();

                if (lastRow > ChunkReader->StartRow) {
                    blockHeap.push_back(TBlockInfo(
                        protoBlock.block_index(),
                        blockIndex,
                        channelIdx,
                        lastRow));

                    result.push_back(protoBlock.block_index());
                    StartRows.push_back(startRow);
                    break;
                }
            } while (true);
        }
    }

    yvector<int> GetBlockReadingOrder()
    {
        yvector<int> result;
        yvector<TBlockInfo> blockHeap;

        SelectOpeningBlocks(result, blockHeap);

        std::make_heap(blockHeap.begin(), blockHeap.end());

        while (true) {
            TBlockInfo currentBlock = blockHeap.front();
            int nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = Attributes.chunk_channels(currentBlock.ChannelIndex);

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            if (nextBlockIndex < protoChannel.blocks_size()) {
                if (currentBlock.LastRow >= ChunkReader->EndRow) {
                    FOREACH (auto& block, blockHeap) {
                        YASSERT(block.LastRow >= ChunkReader->EndRow);
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
            } else {
                // EndRow is not set, so we reached the end of the chunk.
                ChunkReader->EndRow = currentBlock.LastRow;
                FOREACH (auto& block, blockHeap) {
                    YASSERT(ChunkReader->EndRow == block.LastRow);
                }
                break;
            }
        }

        return result;
    }

    void OnFirstBlock(TError error, int selectedChannelIndex)
    {
        if (!error.IsOK()) {
            ChunkReader->State.Fail(error);
            return;
        }

        auto& channelIdx = SelectedChannels[selectedChannelIndex];
        ChunkReader->ChannelReaders.push_back(TChannelReader(ChunkChannels[channelIdx]));

        auto& channelReader = ChunkReader->ChannelReaders.back();
        channelReader.SetBlock(ChunkReader->Codec->Decompress(
            ChunkReader->SequentialReader->GetBlock()));

        for (int row = StartRows[selectedChannelIndex]; 
            row < ChunkReader->StartRow; 
            ++row) 
        {
            YVERIFY(channelReader.NextRow());
        }

        ++selectedChannelIndex;
        if (selectedChannelIndex < SelectedChannels.ysize()) {
            ChunkReader->SequentialReader->AsyncNextBlock()->Subscribe(
                FromMethod(&TInitializer::OnFirstBlock, TPtr(this), selectedChannelIndex)
                    ->Via(ReaderThread->GetInvoker()));
        } else {
            // Initialization complete.
            ChunkReader->Initializer.Reset();
            ChunkReader->State.FinishOperation();
        }
    }

    TSequentialReader::TConfig::TPtr SequentialConfig;
    NChunkClient::IAsyncReader::TPtr AsyncReader;
    TChunkReader::TPtr ChunkReader;

    NProto::TTableChunkAttributes Attributes;
    yvector<TChannel> ChunkChannels;
    yvector<int> SelectedChannels;

    //! First row of the first block in each selected channel.
    /*!
     *  Is used to set channel readers to ChunkReader's StartRow during initialization.
     */
    yvector<int> StartRows;
};

////////////////////////////////////////////////////////////////////////////////

TChunkReader::TChunkReader(
    TSequentialReader::TConfig* config,
    const TChannel& channel,
    NChunkClient::IAsyncReader* chunkReader,
    int startRow,
    int endRow)
    : Codec(NULL)
    , SequentialReader(NULL)
    , Channel(channel)
    , IsColumnValid(false)
    , IsRowValid(false)
    , CurrentRow(-1)
    , StartRow(startRow)
    , EndRow(endRow)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(chunkReader);

    Initializer = New<TInitializer>(config, this, chunkReader);
}

TAsyncError::TPtr TChunkReader::AsyncOpen()
{
    State.StartOperation();

    Initializer->Initialize();

    return State.GetOperationError();
}

bool TChunkReader::HasNextRow() const
{
    // No thread affinity - called from SetCurrentChunk of TChunkSequenceReader.
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    return CurrentRow < EndRow - 1;
}

TAsyncError::TPtr TChunkReader::AsyncNextRow()
{
    // No thread affinity - called from SetCurrentChunk of TChunkSequenceReader.
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    CurrentChannel = 0;
    IsColumnValid = false;
    UsedColumns.clear();
    ++CurrentRow;

    YASSERT(CurrentRow < EndRow);

    State.StartOperation();

    ContinueNextRow(TError(), -1);

    return State.GetOperationError();
}

void TChunkReader::ContinueNextRow(
    TError error,
    int channelIndex)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    if (channelIndex >= 0) {
        auto& channel = ChannelReaders[channelIndex];
        channel.SetBlock(Codec->Decompress(
            SequentialReader->GetBlock()));
    }

    ++channelIndex;

    while (channelIndex < ChannelReaders.ysize()) {
        auto& channel = ChannelReaders[channelIndex];
        if (!channel.NextRow()) {
            YASSERT(SequentialReader->HasNext());
            SequentialReader->AsyncNextBlock()->Subscribe(FromMethod(
                &TChunkReader::ContinueNextRow,
                TPtr(this),
                channelIndex));
            return;
        }
        ++channelIndex;
    }

    IsRowValid = true;
    State.FinishOperation();
}

bool TChunkReader::NextColumn()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);
    YASSERT(IsRowValid);

    while (true) {
        if (CurrentChannel >= ChannelReaders.ysize()) {
            IsColumnValid = false;
            return false;
        }

        auto& channelReader = ChannelReaders[CurrentChannel];
        if (channelReader.NextColumn()) {
            CurrentColumn = channelReader.GetColumn();
            if (!Channel.Contains(CurrentColumn)) {
                continue;
            }

            if (UsedColumns.has(CurrentColumn)) {
                continue;
            }

            UsedColumns.insert(CurrentColumn);
            IsColumnValid = true;
            return true;
        } else {
            ++CurrentChannel;
        }
    }

    YUNREACHABLE();
}

TColumn TChunkReader::GetColumn() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    YASSERT(IsRowValid);
    YASSERT(IsColumnValid);

    return CurrentColumn;
}

TValue TChunkReader::GetValue() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    YASSERT(IsRowValid);
    YASSERT(IsColumnValid);

    return ChannelReaders[CurrentChannel].GetValue();
}

void TChunkReader::Cancel(const TError& error)
{
    State.Fail(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
