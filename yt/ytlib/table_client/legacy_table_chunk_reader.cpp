#include "stdafx.h"

#include "legacy_table_chunk_reader.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "legacy_channel_reader.h"
#include "name_table.h"
#include "private.h"
#include "helpers.h"

#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/misc/finally.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TChannel;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TChannel MakeChannel(const TColumnFilter& columnFilter, TNameTablePtr nameTable)
{
    if (columnFilter.All) {
        return TChannel::Universal();
    }

    TChannel channel = TChannel::Empty();
    for (auto index : columnFilter.Indexes) {
        auto name = ToString(nameTable->GetName(index));
        channel.AddColumn(name);
    }
    return channel;
}

////////////////////////////////////////////////////////////////////////////////

//! Helper class aimed to asynchronously initialize the internals of a chunk reader.
class TLegacyTableChunkReader::TInitializer
    : public TRefCounted
{
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

public:
    TInitializer(
        TSequentialReaderConfigPtr config,
        TLegacyTableChunkReader* tableChunkReader,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        TChannel channel,
        const TKeyColumns& keyColumns,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit)
        : SequentialConfig_(std::move(config))
        , UnderlyingReader_(std::move(chunkReader))
        , BlockCache_(std::move(blockCache))
        , TableChunkReader_(std::move(tableChunkReader))
        , Channel_(channel)
        , LowerLimit_(lowerLimit)
        , UpperLimit_(upperLimit)
        , ReaderKeyColumns_(keyColumns)
        , Logger(tableChunkReader->Logger)
    { }

    void Initialize()
    {
        auto tableChunkReader = TableChunkReader_.Lock();
        YCHECK(tableChunkReader);

        TFinallyGuard finallyGuard([=] () {
            tableChunkReader->Initializer_.Reset();
        });

        try {
            std::vector<int> extensionTags{
                TProtoExtensionTag<TMiscExt>::Value,
                TProtoExtensionTag<TChannelsExt>::Value
            };

            auto hasRangeRequest = LowerLimit_.HasKey() || UpperLimit_.HasKey();

            if (hasRangeRequest) {
                extensionTags.push_back(TProtoExtensionTag<TIndexExt>::Value);
            }

            if (hasRangeRequest || !ReaderKeyColumns_.empty()) {
                extensionTags.push_back(TProtoExtensionTag<TKeyColumnsExt>::Value);
            }

            LOG_DEBUG("Requesting chunk meta");

            auto chunkMeta = WaitFor(UnderlyingReader_->GetMeta(Null, extensionTags))
                    .ValueOrThrow();

            LOG_DEBUG("Chunk meta received");

            // ToDo(psushin): consider throwing.
            YCHECK(EChunkType(chunkMeta.type()) == EChunkType::Table);
            YCHECK(ETableChunkFormat(chunkMeta.version()) == ETableChunkFormat::Old);

            auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
            tableChunkReader->EndRowIndex_ = miscExt.row_count();

            if (LowerLimit_.HasRowIndex()) {
                tableChunkReader->BeginRowIndex_ = std::max(
                    tableChunkReader->BeginRowIndex_,
                    LowerLimit_.GetRowIndex());
            }

            if (UpperLimit_.HasRowIndex()) {
                tableChunkReader->EndRowIndex_ = std::min(
                    tableChunkReader->EndRowIndex_,
                    UpperLimit_.GetRowIndex());
            }

            if (hasRangeRequest) {
                ApplyKeyLimits(tableChunkReader, miscExt, chunkMeta);
            }

            LOG_DEBUG("Reading rows %v-%v",
                tableChunkReader->BeginRowIndex_,
                tableChunkReader->EndRowIndex_);

            tableChunkReader->CurrentRowIndex_ = tableChunkReader->BeginRowIndex_;
            if (tableChunkReader->CurrentRowIndex_ >= tableChunkReader->EndRowIndex_) {
                LOG_WARNING("Nothing to read from current chunk");
                return;
            }

            ChannelsExt_ = GetProtoExtension<TChannelsExt>(chunkMeta.extensions());

            SelectChannels(tableChunkReader);
            YCHECK(SelectedChannels_.size() > 0);

            LOG_DEBUG("Selected channels [%v]", JoinToString(SelectedChannels_));

            auto blockSequence = GetBlockReadSequence(tableChunkReader);
            LOG_DEBUG("Reading %v blocks", blockSequence.size());

            tableChunkReader->SequentialReader_ = New<TSequentialReader>(
                SequentialConfig_,
                std::move(blockSequence),
                UnderlyingReader_,
                BlockCache_,
                NCompression::ECodec(miscExt.compression_codec()));

            tableChunkReader->ChannelReaders_.reserve(SelectedChannels_.size());

            InitFirstBlocks(tableChunkReader);
            if (LowerLimit_.HasKey()) {
                tableChunkReader->SkipToKey(LowerLimit_.GetKey());
            }

            LOG_DEBUG("Reader successfully initilized");
        } catch (const std::exception& ex) {
            LOG_WARNING(ex, "Chunk reader initialization failed");
            throw;
        }
    }

private:
    void SelectChannels(TLegacyTableChunkReaderPtr chunkReader)
    {
        ChunkChannels_.reserve(ChannelsExt_.items_size());
        for (int i = 0; i < ChannelsExt_.items_size(); ++i) {
            ChunkChannels_.push_back(NYT::FromProto<TChannel>(ChannelsExt_.items(i).channel()));
        }

        // Heuristic: first try to find a channel containing the whole read channel.
        // If several exists, choose the one with the minimum number of blocks.
        if (SelectSingleChannel(chunkReader))
            return;

        auto remainder = Channel_;
        for (int channelIdx = 0; channelIdx < ChunkChannels_.size(); ++channelIdx) {
            const auto& currentChannel = ChunkChannels_[channelIdx];
            if (currentChannel.Overlaps(remainder)) {
                remainder -= currentChannel;
                SelectedChannels_.push_back(channelIdx);
                if (remainder.IsEmpty()) {
                    break;
                }
            }
        }
    }

    bool SelectSingleChannel(TLegacyTableChunkReaderPtr chunkReader)
    {
        int resultIdx = -1;
        size_t minBlockCount = std::numeric_limits<size_t>::max();

        for (int i = 0; i < ChunkChannels_.size(); ++i) {
            auto& channel = ChunkChannels_[i];
            if (channel.Contains(Channel_)) {
                size_t blockCount = ChannelsExt_.items(i).blocks_size();
                if (minBlockCount > blockCount) {
                    resultIdx = i;
                    minBlockCount = blockCount;
                }
            }
        }

        if (resultIdx < 0)
            return false;

        SelectedChannels_.push_back(resultIdx);
        return true;
    }

    void SelectOpeningBlocks(
        TLegacyTableChunkReaderPtr chunkReader,
        std::vector<TSequentialReader::TBlockInfo>& result,
        std::vector<TBlockInfo>& blockHeap)
    {
        for (auto channelIdx : SelectedChannels_) {
            const auto& protoChannel = ChannelsExt_.items(channelIdx);
            int blockIndex = -1;
            i64 startRow = 0;
            i64 lastRow = 0;
            while (true) {
                ++blockIndex;
                YCHECK(blockIndex < static_cast<int>(protoChannel.blocks_size()));
                const auto& protoBlock = protoChannel.blocks(blockIndex);

                startRow = lastRow;
                lastRow += protoBlock.row_count();

                if (lastRow > chunkReader->BeginRowIndex_) {
                    blockHeap.push_back(TBlockInfo(
                        protoBlock.block_index(),
                        blockIndex,
                        channelIdx,
                        lastRow));

                    result.push_back(TSequentialReader::TBlockInfo(
                        protoBlock.block_index(),
                        protoBlock.uncompressed_size()));
                    StartRows_.push_back(startRow);
                    break;
                }
            }
        }
    }

    std::vector<TSequentialReader::TBlockInfo> GetBlockReadSequence(TLegacyTableChunkReaderPtr chunkReader)
    {
        std::vector<TSequentialReader::TBlockInfo> result;
        std::vector<TBlockInfo> blockHeap;

        SelectOpeningBlocks(chunkReader, result, blockHeap);

        std::make_heap(blockHeap.begin(), blockHeap.end());

        while (true) {
            auto currentBlock = blockHeap.front();
            auto nextBlockIndex = currentBlock.ChannelBlockIndex + 1;
            const auto &protoChannel = ChannelsExt_.items(currentBlock.ChannelIndex);
            auto lastRow = currentBlock.LastRow;

            std::pop_heap(blockHeap.begin(), blockHeap.end());
            blockHeap.pop_back();

            YCHECK(nextBlockIndex <= protoChannel.blocks_size());

            if (currentBlock.LastRow >= chunkReader->EndRowIndex_) {
                for (auto &block : blockHeap) {
                    YASSERT(block.LastRow >= chunkReader->EndRowIndex_);
                }
                break;
            }

            while (nextBlockIndex < protoChannel.blocks_size()) {
                const auto &protoBlock = protoChannel.blocks(nextBlockIndex);

                lastRow += protoBlock.row_count();
                blockHeap.push_back(TBlockInfo(
                    protoBlock.block_index(),
                    nextBlockIndex,
                    currentBlock.ChannelIndex,
                    lastRow));

                std::push_heap(blockHeap.begin(), blockHeap.end());
                result.push_back(TSequentialReader::TBlockInfo(
                    protoBlock.block_index(),
                    protoBlock.uncompressed_size()));
                break;
            }
        }

        return result;
    }

    void ApplyKeyLimits(
        TLegacyTableChunkReaderPtr tableChunkReader,
        const TMiscExt& miscExt,
        const TChunkMeta& chunkMeta)
    {
        if (!miscExt.sorted()) {
            THROW_ERROR_EXCEPTION("Received key range read request for an unsorted chunk %v",
                UnderlyingReader_->GetChunkId());
        }

        auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);
        ValidateKeyColumns(ReaderKeyColumns_, chunkKeyColumns);

        tableChunkReader->EmptyKey_.resize(chunkKeyColumns.size());
        tableChunkReader->CurrentKey_.resize(chunkKeyColumns.size());
        auto nameTable = tableChunkReader->GetNameTable();
        for (int i = 0; i < chunkKeyColumns.size(); ++i) {
            auto id = nameTable->GetIdOrRegisterName(chunkKeyColumns[i]);
            auto& columnInfo = tableChunkReader->GetColumnInfo(id);
            columnInfo.ChunkKeyIndex = i;

            tableChunkReader->EmptyKey_[i] = MakeUnversionedSentinelValue(EValueType::Null, id);

            Channel_.AddColumn(chunkKeyColumns[i]);
        }

        auto indexExt = GetProtoExtension<TIndexExt>(chunkMeta.extensions());

        if (LowerLimit_.HasKey()) {
            typedef decltype(indexExt.items().begin()) TIter;
            std::reverse_iterator<TIter> rbegin(indexExt.items().end());
            std::reverse_iterator<TIter> rend(indexExt.items().begin());
            auto it = std::upper_bound(
                rbegin,
                rend,
                LowerLimit_.GetKey(),
                [] (const TOwningKey& key, const TIndexRow& row) {
                    auto indexKey = NYT::FromProto<TOwningKey>(row.key());
                    return CompareRows(key, indexKey) > 0;
                });

            if (it != rend) {
                tableChunkReader->BeginRowIndex_ = std::max(
                    it->row_index() + 1,
                    tableChunkReader->BeginRowIndex_);
            }
        }

        if (UpperLimit_.HasKey()) {
            auto it = std::upper_bound(
                indexExt.items().begin(),
                indexExt.items().end(),
                UpperLimit_.GetKey(),
                [] (const TOwningKey& key, const TIndexRow& row) {
                    auto indexKey = NYT::FromProto<TOwningKey>(row.key());
                    return CompareRows(key, indexKey) < 0;
                });

            if (it != indexExt.items().end()) {
                tableChunkReader->EndRowIndex_ = std::min(
                    it->row_index(),
                    tableChunkReader->EndRowIndex_);
            }
        }
    }

    void InitFirstBlocks(TLegacyTableChunkReaderPtr tableChunkReader)
    {
        for (int selectedChannelIndex = 0; selectedChannelIndex < SelectedChannels_.size(); ++selectedChannelIndex) {
            YCHECK(tableChunkReader->SequentialReader_->HasMoreBlocks());
            WaitFor(tableChunkReader->SequentialReader_->FetchNextBlock())
                .ThrowOnError();

            auto channelIndex = SelectedChannels_[selectedChannelIndex];
            tableChunkReader->ChannelReaders_.push_back(New<TLegacyChannelReader>(ChunkChannels_[channelIndex]));
            auto& channelReader = tableChunkReader->ChannelReaders_.back();
            auto block = tableChunkReader->SequentialReader_->GetCurrentBlock();
            channelReader->SetBlock(block);

            for (
                i64 rowIndex = StartRows_[selectedChannelIndex];
                rowIndex < tableChunkReader->BeginRowIndex_;
                ++rowIndex)
            {
                YCHECK(channelReader->NextRow());
            }

            LOG_DEBUG("Skipped initial rows for channel %v", channelIndex);
        }

        tableChunkReader->ResetCurrentRow();
        tableChunkReader->MakeAndValidateRow();
    }

    TSequentialReaderConfigPtr SequentialConfig_;
    IChunkReaderPtr UnderlyingReader_;
    IBlockCachePtr BlockCache_;
    TWeakPtr<TLegacyTableChunkReader> TableChunkReader_;

    TChannel Channel_;

    const TReadLimit LowerLimit_;
    const TReadLimit UpperLimit_;

    TKeyColumns ReaderKeyColumns_;

    TChannelsExt ChannelsExt_;
    TChannels ChunkChannels_;
    std::vector<int> SelectedChannels_;

    //! First row of the first block in each selected channel.
    std::vector<i64> StartRows_;

    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TLegacyTableChunkReader::TLegacyTableChunkReader(
    TChunkReaderConfigPtr config,
    const TColumnFilter& columnFilter,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    i64 tableRowIndex,
    i32 rangeIndex)
    : Config_(config)
    , SequentialReader_(nullptr)
    , ColumnFilter_(columnFilter)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , UpperLimit_(upperLimit)
    , MemoryPool_(TLegacyTableChunkReaderMemoryPoolTag())
    , TableRowIndex_(tableRowIndex)
    , RangeIndex_(rangeIndex)
    , Logger(TableClientLogger)
{
    YCHECK(Config_);
    YCHECK(underlyingReader);

    Logger.AddTag("ChunkId: %v", underlyingReader->GetChunkId());

    auto channel = MakeChannel(columnFilter, nameTable);
    for (int i = 0; i < keyColumns.size(); ++i) {
        auto id = NameTable_->GetId(keyColumns[i]);
        auto& columnInfo = GetColumnInfo(id);
        columnInfo.ReaderKeyIndex = i;
        columnInfo.InChannel = true;

        channel.AddColumn(keyColumns[i]);
    }

    if (!columnFilter.All) {
        for (auto id : columnFilter.Indexes) {
            auto& columnInfo = GetColumnInfo(id);
            columnInfo.InChannel = true;
        }
    }

    if (Config_->SamplingRate) {
        RowSampler_ = CreateChunkRowSampler(
            underlyingReader->GetChunkId(),
            Config_->SamplingRate.Get(),
            Config_->SamplingSeed.Get(std::random_device()()));
    }

    Initializer_ = New<TInitializer>(
        Config_,
        this,
        underlyingReader,
        blockCache,
        channel,
        keyColumns,
        lowerLimit,
        upperLimit);
}

TFuture<void> TLegacyTableChunkReader::Open()
{
    ReadyEvent_ = BIND(&TInitializer::Initialize, Initializer_)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
    return ReadyEvent_;
}

bool TLegacyTableChunkReader::Read(std::vector<TUnversionedRow> *rows)
{
    YCHECK(!Initializer_);
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!ReadyEvent_.IsSet()) {
        return true;
    }

    if (!ReadyEvent_.Get().IsOK()) {
        return true;
    }

    if (UnfetchedChannelIndex_ >= 0) {
        if (!ContinueFetchNextRow()) {
            return true;
        }
    }

    if (CurrentRow_.empty()) {
        return false;
    }

    i64 dataWeight = 0;
    while (rows->size() < rows->capacity() && dataWeight < Config_->MaxDataSizePerRead) {
        if (!RowSampler_ || RowSampler_->ShouldTakeRow(GetTableRowIndex())) {
            auto row = TUnversionedRow::Allocate(&MemoryPool_, CurrentRow_.size());
            std::copy(CurrentRow_.begin(), CurrentRow_.end(), row.Begin());
            rows->push_back(row);
            dataWeight += GetDataWeight(row);
            ++RowCount_;
        }

        if (!FetchNextRow() || CurrentRow_.empty()) {
            return true;
        }
    }

    return true;
}

TFuture<void> TLegacyTableChunkReader::GetReadyEvent()
{
    return ReadyEvent_;
}

i64 TLegacyTableChunkReader::GetTableRowIndex() const
{
    return TableRowIndex_ + CurrentRowIndex_;
}

i32 TLegacyTableChunkReader::GetRangeIndex() const
{
    return RangeIndex_;
}

void TLegacyTableChunkReader::ResetCurrentRow()
{
    YASSERT(CurrentKey_.size() == EmptyKey_.size());
    std::memcpy(CurrentKey_.data(), EmptyKey_.data(), EmptyKey_.size() * sizeof(TUnversionedValue));
    CurrentRow_.resize(KeyColumns_.size());
    std::memcpy(CurrentRow_.data(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));
}

void TLegacyTableChunkReader::FinishReader()
{
    LOG_DEBUG("Chunk reader finished");
    CurrentRow_.clear();
    --CurrentRowIndex_;
}

bool TLegacyTableChunkReader::FetchNextRow()
{
    ResetCurrentRow();

    if (++CurrentRowIndex_ == EndRowIndex_) {
        FinishReader();
        return true;
    }

    return DoFetchNextRow();
}

bool TLegacyTableChunkReader::ContinueFetchNextRow()
{
    auto block = SequentialReader_->GetCurrentBlock();
    auto &channel = ChannelReaders_[UnfetchedChannelIndex_];
    channel->SetBlock(block);

    return DoFetchNextRow();
}

bool TLegacyTableChunkReader::DoFetchNextRow()
{
    for (int channelIndex = UnfetchedChannelIndex_ + 1; channelIndex < ChannelReaders_.size(); ++channelIndex) {
        auto& channel = ChannelReaders_[channelIndex];
        if (channel->NextRow()) {
            continue;
        }

        YCHECK(SequentialReader_->HasMoreBlocks());
        ReadyEvent_ = SequentialReader_->FetchNextBlock();
        UnfetchedChannelIndex_ = channelIndex;

        return false;
    }
    UnfetchedChannelIndex_ = -1;
    MakeAndValidateRow();

    return true;
}

void TLegacyTableChunkReader::SkipToKey(const TOwningKey& key)
{
    while (true) {
        if (CurrentRow_.empty()) {
            return;
        }

        if (CompareRows(
                CurrentKey_.data(),
                CurrentKey_.data() + CurrentKey_.size(),
                key.Begin(),
                key.End()) >= 0)
        {
            return;
        }

        if (FetchNextRow()) {
            continue;
        }

        do {
            WaitFor(ReadyEvent_)
                .ThrowOnError();
        } while (!ContinueFetchNextRow());
    }
}

void TLegacyTableChunkReader::MakeAndValidateRow()
{
    for (const auto& channel : ChannelReaders_) {
        while (channel->NextColumn()) {
            auto id = NameTable_->GetIdOrRegisterName(channel->GetColumn());
            auto& columnInfo = GetColumnInfo(id);
            if (columnInfo.RowIndex < CurrentRowIndex_) {
                columnInfo.RowIndex = CurrentRowIndex_;

                auto value = MakeUnversionedValue(channel->GetValue(), id, Lexer_);
                if (columnInfo.ChunkKeyIndex >= 0) {
                    CurrentKey_[columnInfo.ChunkKeyIndex] = value;
                }

                if (columnInfo.ReaderKeyIndex >= 0) {
                    CurrentRow_[columnInfo.ReaderKeyIndex] = value;
                } else if (columnInfo.InChannel) {
                    CurrentRow_.push_back(value);
                }
            }
        }
    }

    if (!UpperLimit_.HasKey()) {
        return;
    }

    if (CompareRows(
        CurrentKey_.data(),
        CurrentKey_.data() + CurrentKey_.size(),
        UpperLimit_.GetKey().Begin(),
        UpperLimit_.GetKey().End()) >= 0)
    {
        FinishReader();
    }
}

TLegacyTableChunkReader::TColumnInfo& TLegacyTableChunkReader::GetColumnInfo(int id)
{
    if (id < ColumnInfo_.size()) {
        return ColumnInfo_[id];
    }

    while (id >= ColumnInfo_.size()) {
        TColumnInfo info;
        info.InChannel = ColumnFilter_.All;
        ColumnInfo_.push_back(info);
    }

    return ColumnInfo_.back();
}

TDataStatistics TLegacyTableChunkReader::GetDataStatistics() const
{
    TDataStatistics result = ZeroDataStatistics();
    result.set_chunk_count(1);

    if (SequentialReader_) {
        result.set_row_count(RowCount_);
        result.set_uncompressed_data_size(SequentialReader_->GetUncompressedDataSize());
        result.set_compressed_data_size(SequentialReader_->GetCompressedDataSize());
    }

    return result;
}

TFuture<void> TLegacyTableChunkReader::GetFetchingCompletedEvent()
{
    if (SequentialReader_) {
        return SequentialReader_->GetFetchingCompletedEvent();
    } else {
        // Error occured during initialization.
        return VoidFuture;
    }
}

TNameTablePtr TLegacyTableChunkReader::GetNameTable() const
{
    return NameTable_;
}

TKeyColumns TLegacyTableChunkReader::GetKeyColumns() const
{
    return KeyColumns_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
