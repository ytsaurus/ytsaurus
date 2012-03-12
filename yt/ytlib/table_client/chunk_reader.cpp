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

struct IValidator
{
    virtual bool IsValid(const TKey& key) = 0;
    virtual ~IValidator() { }
};

////////////////////////////////////////////////////////////////////////////////

class TNullValidator
    : public IValidator
{
    bool IsValid(const TKey& key)
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TComparator>
class TGenericValidator
    : public IValidator
{
public:
    TGenericValidator(const TKey& key)
        : Key(key)
    { }

    bool IsValid(const TKey& key)
    {
        return Comparator(key, Key);
    }

private:
    TKey Key;
    TComparator Comparator;
};

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

template <class TComparator>
struct TProtoKeyCompare
{
    bool operator()(const TKey& key, const NProto::TKeySample& sample)
    {
        auto sampleKey = FromProto<Stroka>(sample.key().values());
        return Comparator(key, sampleKey);
    }

    TComparator Comparator;
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
        TChunkReader* chunkReader, 
        NChunkClient::IAsyncReader* asyncReader,
        const NProto::TReadLimit& startLimit,
        const NProto::TReadLimit& endLimit)
        : SequentialConfig(config)
        , AsyncReader(asyncReader)
        , ChunkReader(chunkReader)
        , Channel(chunkReader->Channel)
    { }

    void Initialize()
    {
        AsyncReader->AsyncGetChunkInfo()->Subscribe(FromMethod(
            &TInitializer::OnGotMeta, 
            MakeStrong(this))->Via(ReaderThread->GetInvoker()));
    }

private:
    void OnFail(TError& error, TChunkReader::TPtr chunkReader) 
    {
        chunkReader->Initializer.Reset();
        chunkReader->State.Fail(error);
    }

    void OnGotMeta(NChunkClient::IAsyncReader::TGetInfoResult result)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        if (!result.IsOK()) {
            LOG_WARNING("Failed to download chunk meta: %s", ~result.GetMessage());
            OnFail(result, chunkReader);
            return;
        }

        FOREACH(auto& column, Channel.GetColumns()) {
            auto& columnInfo = chunkReader->FixedColumns[column];
            columnInfo.InChannel = true;
        }

        StartValidator.Reset(new TNullValidator());
        chunkReader->EndValidator.Reset(new TNullValidator());

        Attributes = result.Value().attributes().GetExtension(
            NProto::TTableChunkAttributes::table_attributes);

        StartRowIndex = 0;
        chunkReader->EndRowIndex = Attributes.row_count();
        if (StartLimit.has_row_index())
            StartRowIndex = std::max(StartRowIndex, StartLimit.row_index());

        if (EndLimit.has_row_index())
            chunkReader->EndRowIndex = std::min(chunkReader->EndRowIndex, EndLimit.row_index());

        if (StartLimit.has_key() || EndLimit.has_key()) {
            // We expect sorted chunk here.
            if (!Attributes.is_sorted()) {
                LOG_WARNING("Received key range read request to unsorted chunk.");
                OnFail(
                    TError("Received key range read request to unsorted chunk."), 
                    chunkReader);
                return;
            }

            auto keyColumns = FromProto<TColumn>(Attributes.key_columns());
            for (int i = 0; i < keyColumns.size(); ++i) {
                auto& column = keyColumns[i];
                Channel.AddColumn(column);
                auto& columnInfo = chunkReader->FixedColumns[column];
                columnInfo.KeyIndex = i;
            }

            if (StartLimit.has_key()) {
                TKey key = FromProto<Stroka>(StartLimit.key().values());

                // define start row
                if (key.size() == 0) {
                    // Do nothing - range starts from -inf.
                } else if (keyColumns.size() == 0) {
                    // Empty row set selected: requested finite or 
                    // semifinite range on table with empty key.
                    chunkReader->EndRowIndex = 0;
                } else {
                    StartValidator.Reset(new TGenericValidator< std::greater_equal<TKey> >(
                        key));

                    std::reverse_iterator<decltype(Attributes.key_samples().begin())> rbegin(Attributes.key_samples().begin());
                    std::reverse_iterator<decltype(Attributes.key_samples().end())> rend(Attributes.key_samples().end());
                    auto it = std::upper_bound(
                        rbegin, 
                        rend, 
                        key, 
                        TProtoKeyCompare< std::less<TKey> >());

                    if (it != rend) {
                        StartRowIndex = std::max(it->row_index() + 1, StartRowIndex);
                    }
                }
            }

            if (EndLimit.has_key()) {
                auto key = FromProto<Stroka>(EndLimit.key().values());

                // define end row
                if (key.size() == 0) {
                    // Do nothing - range ends at +inf.
                } else if (keyColumns.size() == 0) {
                    // Empty row set selected: requested finite or 
                    // semifinite range on table with empty key.
                    chunkReader->EndRowIndex = 0;
                } else {
                    chunkReader->EndValidator.Reset(
                        new TGenericValidator< std::less<TKey> >(key));

                    auto it = std::upper_bound(
                        Attributes.key_samples().begin(), 
                        Attributes.key_samples().end(), 
                        key, 
                        TProtoKeyCompare< std::greater<TKey> >());

                    if (it != Attributes.key_samples().end()) {
                        chunkReader->EndRowIndex = std::min(
                            it->row_index(), 
                            chunkReader->EndRowIndex);
                    }
                }
            }
        }

        chunkReader->CurrentRowIndex = StartRowIndex;
        if (chunkReader->EndRowIndex >= chunkReader->CurrentRowIndex) {
            LOG_WARNING("Nothing to read from current chunk.");
            chunkReader->State.FinishOperation();
            return;
        }

        chunkReader->Codec = GetCodec(ECodecId(Attributes.codec_id()));

        SelectChannels(chunkReader);
        YASSERT(SelectedChannels.size() > 0);

        yvector<int> blockIndexSequence = GetBlockReadingOrder(chunkReader);
        chunkReader->SequentialReader = New<TSequentialReader>(
            ~SequentialConfig,
            blockIndexSequence,
            ~AsyncReader);

        chunkReader->ChannelReaders.reserve(SelectedChannels.size());

        chunkReader->SequentialReader->AsyncNextBlock()->Subscribe(FromMethod(
            &TInitializer::OnFirstBlock,
            MakeStrong(this),
            0)->Via(ReaderThread->GetInvoker()));
    }

    void SelectChannels(TChunkReader::TPtr chunkReader)
    {
        ChunkChannels.reserve(Attributes.chunk_channels_size());
        for(int i = 0; i < Attributes.chunk_channels_size(); ++i) {
            ChunkChannels.push_back(TChannel::FromProto(Attributes.chunk_channels(i).channel()));
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

    bool SelectSingleChannel(TChunkReader::TPtr chunkReader)
    {
        int resultIdx = -1;
        size_t minBlockCount = std::numeric_limits<size_t>::max();

        for (int i = 0; i < Attributes.chunk_channels_size(); ++i) {
            auto& channel = ChunkChannels[i];
            if (channel.Contains(Channel)) {
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

    void SelectOpeningBlocks(
        TChunkReader::TPtr chunkReader,
        yvector<int>& result, 
        yvector<TBlockInfo>& blockHeap) 
    {
        FOREACH (auto channelIdx, SelectedChannels) {
            const auto& protoChannel = Attributes.chunk_channels(channelIdx);
            int blockIndex = -1;
            int startRow = 0;
            int lastRow = 0;
            while (true) {
                ++blockIndex;
                YASSERT(blockIndex < (int)protoChannel.blocks_size());
                const auto& protoBlock = protoChannel.blocks(blockIndex);
                // When a new block is set in TChannelReader, reader is virtually 
                // one row behind its real starting row. E.g. for the first row of 
                // the channel we consider start row to be -1.
                startRow = lastRow - 1;
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

    yvector<int> GetBlockReadingOrder(TChunkReader::TPtr chunkReader)
    {
        yvector<int> result;
        yvector<TBlockInfo> blockHeap;

        SelectOpeningBlocks(chunkReader, result, blockHeap);

        std::make_heap(blockHeap.begin(), blockHeap.end());

        while (true) {
            TBlockInfo currentBlock = blockHeap.front();
            int nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto& protoChannel = Attributes.chunk_channels(currentBlock.ChannelIndex);

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

    void OnFirstBlock(TError error, int selectedChannelIndex)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        auto& channelIdx = SelectedChannels[selectedChannelIndex];

        if (!error.IsOK()) {
            LOG_WARNING("Failed to download first block in channel (channelIndex: %d, error: %s)", 
                channelIdx,
                ~error.GetMessage());
            chunkReader->State.Fail(error);
            return;
        }

        chunkReader->ChannelReaders.push_back(TChannelReader(ChunkChannels[channelIdx]));

        auto& channelReader = chunkReader->ChannelReaders.back();
        channelReader.SetBlock(chunkReader->Codec->Decompress(
            chunkReader->SequentialReader->GetBlock()));

        for (int row = StartRows[selectedChannelIndex]; 
            row < StartRowIndex; 
            ++row) 
        {
            YVERIFY(channelReader.NextRow());
        }

        ++selectedChannelIndex;
        if (selectedChannelIndex < SelectedChannels.size()) {
            chunkReader->SequentialReader->AsyncNextBlock()->Subscribe(
                FromMethod(
                    &TInitializer::OnFirstBlock, 
                    MakeWeak(this), 
                    selectedChannelIndex)
                ->Via(ReaderThread->GetInvoker()));
        } else {
            // Create current row.
            chunkReader->MakeCurrentRow();
            ValidateRow(TError());
        }
    }

    void ValidateRow(TError error)
    {
        auto chunkReader = ChunkReader.Lock();
        if (!chunkReader)
            return;

        YASSERT(chunkReader->CurrentRowIndex < chunkReader->EndRowIndex);
        if (!StartValidator->IsValid(chunkReader->CurrentKey)) {
            chunkReader->AsyncNextRow()->Subscribe(FromMethod(
                &TInitializer::ValidateRow,
                MakeWeak(this)));
            return;
        }

        // Initialization complete.
        chunkReader->Initializer.Reset();
        chunkReader->State.FinishOperation();
    }

    TSequentialReader::TConfig::TPtr SequentialConfig;
    NChunkClient::IAsyncReader::TPtr AsyncReader;
    TWeakPtr<TChunkReader> ChunkReader;

    TChannel Channel;

    NProto::TReadLimit StartLimit;
    NProto::TReadLimit EndLimit;

    i64 StartRowIndex;

    THolder<IValidator> StartValidator;

    NProto::TTableChunkAttributes Attributes;
    std::vector<TChannel> ChunkChannels;
    std::vector<int> SelectedChannels;

    //! First row of the first block in each selected channel.
    /*!
     *  Is used to set channel readers to ChunkReader's StartRow during initialization.
     */
    std::vector<int> StartRows;
};

////////////////////////////////////////////////////////////////////////////////

TChunkReader::TChunkReader(
    TSequentialReader::TConfig* config,
    const TChannel& channel,
    NChunkClient::IAsyncReader* chunkReader,
    const NProto::TReadLimit& startLimit,
    const NProto::TReadLimit& endLimit)
    : Codec(NULL)
    , SequentialReader(NULL)
    , Channel(channel)
    , CurrentRowIndex(-1)
    , EndRowIndex(0)
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

TAsyncError::TPtr TChunkReader::AsyncOpen()
{
    State.StartOperation();

    Initializer->Initialize();

    return State.GetOperationError();
}

TAsyncError::TPtr TChunkReader::AsyncNextRow()
{
    // No thread affinity - called from SetCurrentChunk of TChunkSequenceReader.
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    UsedRangeColumns.clear();
    FOREACH(auto& it, FixedColumns) {
        it.Second().Used = false;
    }
    CurrentRow.clear();
    CurrentKey.assign(CurrentKey.size(), Stroka());

    CurrentRowIndex = std::min(CurrentRowIndex + 1, EndRowIndex);

    if (CurrentRowIndex == EndRowIndex)
        return State.GetOperationError();

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

    while (channelIndex < ChannelReaders.size()) {
        auto& channel = ChannelReaders[channelIndex];
        if (!channel.NextRow()) {
            YASSERT(SequentialReader->HasNext());
            SequentialReader->AsyncNextBlock()->Subscribe(FromMethod(
                &TChunkReader::ContinueNextRow,
                TWeakPtr<TChunkReader>(this),
                channelIndex));
            return;
        }
        ++channelIndex;
    }

    MakeCurrentRow();

    State.FinishOperation();
}

void TChunkReader::MakeCurrentRow()
{
    FOREACH(auto& channel, ChannelReaders) {
        while (channel.NextColumn()) {
            auto column = channel.GetColumn();
            auto it = FixedColumns.find(column);
            if (it != FixedColumns.end()) {
                auto& columnInfo = it->Second();
                if (!columnInfo.Used) {
                    columnInfo.Used = true;
                    if (columnInfo.KeyIndex > 0) {
                        CurrentKey[columnInfo.KeyIndex] = channel.GetValue().ToString();
                    }
                    if (columnInfo.InChannel) {
                        CurrentRow.push_back(std::make_pair(column, channel.GetValue()));
                    }
                }
            } else if (UsedRangeColumns.insert(column).Second() && 
                Channel.ContainsInRanges(column)) 
            {
                CurrentRow.push_back(std::make_pair(column, channel.GetValue()));
            }
        }
    }
}

const TRow& TChunkReader::GetCurrentRow() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!Initializer);

    return CurrentRow;
}

bool TChunkReader::IsValid() const
{
    if (CurrentRowIndex < EndRowIndex)
        return EndValidator->IsValid(CurrentKey);
    else
        return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
