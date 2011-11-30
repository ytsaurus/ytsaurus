#include "stdafx.h"
#include "chunk_reader.h"

#include "table_chunk_meta.pb.h"

#include "../misc/foreach.h"
#include "../misc/sync.h"
#include "../misc/serialize.h"
#include "../actions/action_util.h"

#include <algorithm>
#include <limits>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct TBlockInfo {
    int ChunkBlockIndex;
    int ChannelBlockIndex;
    int ChannelIndex;
    int LastRow;

    bool operator< (const TBlockInfo& rhs)
    {
        return (LastRow > rhs.LastRow) || 
            (LastRow == rhs.LastRow && ChannelIndex > rhs.ChannelIndex);
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
class TChunkReader::TInitializer:
    public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TInitializer> TPtr;

    TInitializer(
        const TSequentialReader::TConfig& config,
        TChunkReader::TPtr chunkReader, 
        NChunkClient::IAsyncReader::TPtr asyncReader)
        : SequentialConfig(config)
        , AsyncReader(asyncReader)
        , ChunkReader(chunkReader)
    { }

    void Initialize()
    {
        // The last block contains meta.
        yvector<int> metaIndex(1, -1);
        AsyncReader->AsyncReadBlocks(metaIndex)
            ->Subscribe(FromMethod(
                &TInitializer::OnGotMeta, 
                TPtr(this))
            ->Via(ReaderThread->GetInvoker()));
    }

private:
    void OnGotMeta(NChunkClient::IAsyncReader::TReadResult readResult)
    {
        if (!readResult.Error.IsOK()) {
            ChunkReader->State.Fail(readResult.Error.GetMessage());
            return;
        }

        auto& metaBlob = readResult.Blocks.front();

        DeserializeProtobuf(&ProtoMeta, metaBlob);

        SelectChannels();
        YASSERT(SelectedChannels.size() > 0);

        yvector<int> blockIndexSequence = GetBlockReadingOrder();
        ChunkReader->SequentialReader = 
            New<TSequentialReader>(SequentialConfig, blockIndexSequence, ~AsyncReader);

        ChunkReader->ChannelReaders.reserve(SelectedChannels.size());

        ChunkReader->SequentialReader->AsyncNextBlock()->Subscribe(
            FromMethod(&TInitializer::OnFirstBlock, TPtr(this), 0)
        );
    }

    void SelectChannels()
    {
        ChunkChannels.reserve(ProtoMeta.ChannelsSize());
        for(size_t i = 0; i < ProtoMeta.ChannelsSize(); ++i) {
            ChunkChannels.push_back(TChannel::FromProto(ProtoMeta.GetChannels(i)));
        }

        // Heuristic: first try to find a channel that contain the whole read channel.
        // If several exists, choose the one with the minimum number of blocks.
        if (SelectSingleChannel())
            return;

        TChannel remainder = ChunkReader->Channel;
        for (int channelIdx = 0; channelIdx < ChunkChannels.ysize(); ++channelIdx) {
            auto& curChannel = ChunkChannels[channelIdx];
            if (curChannel.Overlaps(remainder)) {
                remainder -= curChannel;
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

        for (size_t i = 0; i < ProtoMeta.ChannelsSize(); ++i) {
            auto& channel = ChunkChannels[i];
            if (channel.Contains(ChunkReader->Channel)) {
                size_t blockCount = ProtoMeta.GetChannels(i).BlocksSize();
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
            const auto& protoChannel = ProtoMeta.GetChannels(channelIdx);
            int blockIndex = -1;
            int startRow = 0;
            int lastRow = 0;
            do {
                ++blockIndex;
                YASSERT(blockIndex < (int)protoChannel.BlocksSize());
                const auto& protoBlock = protoChannel.GetBlocks(blockIndex);
                // When a new block is set in TChannelReader, reader is virtually 
                // one row behind its real starting row. E.g. for the first row of 
                // the channel we consider start row to be -1.
                startRow = lastRow - 1;
                lastRow += protoBlock.GetRowCount();

                if (lastRow > ChunkReader->StartRow) {
                    blockHeap.push_back(TBlockInfo(
                        protoBlock.GetBlockIndex(),
                        blockIndex,
                        channelIdx,
                        lastRow));

                    result.push_back(protoBlock.GetBlockIndex());
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
            size_t nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = ProtoMeta.GetChannels(currentBlock.ChannelIndex);

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            if (nextBlockIndex < protoChannel.BlocksSize()) {
                if (currentBlock.LastRow >= ChunkReader->EndRow) {
                    FOREACH (auto& block, blockHeap) {
                        YASSERT(block.LastRow >= ChunkReader->EndRow);
                    }
                    break;
                }

                const auto& protoBlock = protoChannel.GetBlocks(nextBlockIndex);

                blockHeap.push_back(TBlockInfo(
                    protoBlock.GetBlockIndex(),
                    nextBlockIndex,
                    currentBlock.ChannelIndex,
                    currentBlock.LastRow + protoBlock.GetRowCount()));

                std::push_heap(blockHeap.begin(), blockHeap.end());
                result.push_back(protoBlock.GetBlockIndex());
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

    void OnFirstBlock(TAsyncStreamState::TResult result, int selectedChannelIndex)
    {
        if (!result.IsOK) {
            ChunkReader->State.Fail(result.ErrorMessage);
            return;
        }

        auto& channelIdx = SelectedChannels[selectedChannelIndex];
        ChunkReader->ChannelReaders.push_back(TChannelReader(ChunkChannels[channelIdx]));

        auto& channelReader = ChunkReader->ChannelReaders.back();
        channelReader.SetBlock(ChunkReader->SequentialReader->GetBlock());

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
            );
        } else {
            // Initialization complete.
            ChunkReader->Initializer.Reset();
            ChunkReader->State.FinishOperation();
        }
    }

    const TSequentialReader::TConfig SequentialConfig;
    NChunkClient::IAsyncReader::TPtr AsyncReader;
    TChunkReader::TPtr ChunkReader;

    NProto::TChunkMeta ProtoMeta;
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
    const TSequentialReader::TConfig& config,
    const TChannel& channel,
    NChunkClient::IAsyncReader::TPtr chunkReader,
    int startRow,
    int endRow)
    : SequentialReader(NULL)
    , Channel(channel)
    , IsColumnValid(false)
    , IsRowValid(false)
    , CurrentRow(-1)
    , StartRow(startRow)
    , EndRow(endRow)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~chunkReader != NULL);
    if (EndRow < 0) {
        // Will be later set by the initializer to the chunk row count.
        EndRow = std::numeric_limits<int>::max();
    }

    Initializer = New<TInitializer>(config, this, chunkReader);
}

TAsyncStreamState::TAsyncResult::TPtr TChunkReader::AsyncOpen()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    State.StartOperation();

    Initializer->Initialize();

    return State.GetOperationResult();
}

bool TChunkReader::HasNextRow() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(~Initializer != NULL);

    return CurrentRow < EndRow - 1;
}

TAsyncStreamState::TAsyncResult::TPtr TChunkReader::AsyncNextRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(~Initializer == NULL);

    CurrentChannel = 0;
    IsColumnValid = false;
    UsedColumns.clear();
    ++CurrentRow;

    YASSERT(CurrentRow < EndRow);

    State.StartOperation();

    ContinueNextRow(TAsyncStreamState::TResult(), -1);

    return State.GetOperationResult();
}

void TChunkReader::ContinueNextRow(
    TAsyncStreamState::TResult result,
    int channelIndex)
{
    if (!result.IsOK) {
        State.Fail(result.ErrorMessage);
        return;
    }

    if (channelIndex >= 0) {
        auto& channel = ChannelReaders[channelIndex];
        channel.SetBlock(SequentialReader->GetBlock());
        ++channelIndex;
    }

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
    YASSERT(~Initializer == NULL);
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
    YASSERT(~Initializer == NULL);

    YASSERT(IsRowValid);
    YASSERT(IsColumnValid);

    return CurrentColumn;
}

TValue TChunkReader::GetValue()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(~Initializer == NULL);

    YASSERT(IsRowValid);
    YASSERT(IsColumnValid);

    return ChannelReaders[CurrentChannel].GetValue();
}

void TChunkReader::Cancel(const Stroka& errorMessage)
{
    State.Fail(errorMessage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
