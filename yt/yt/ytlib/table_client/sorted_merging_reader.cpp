#include "sorted_merging_reader.h"

#include "private.h"
#include "schemaless_multi_chunk_reader.h"
#include "timing_reader.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/misc/heap.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TSortedStream
{
public:
    DEFINE_BYREF_RO_PROPERTY(ISchemalessMultiChunkReaderPtr, Reader);

    DEFINE_BYREF_RO_PROPERTY(TComparator, Comparator);

    DEFINE_BYVAL_RO_PROPERTY(int, StreamIndex);
    DEFINE_BYVAL_RO_PROPERTY(int, TableIndex, -1);

public:
    TSortedStream(
        ISchemalessMultiChunkReaderPtr reader,
        TComparator comparator,
        int streamIndex,
        double fraction)
        : Reader_(std::move(reader))
        , Comparator_(std::move(comparator))
        , StreamIndex_(streamIndex)
        , Fraction_(fraction)
    { }

    TUnversionedRow GetCurrentRow() const
    {
        YT_ASSERT(!Exhausted());

        return Rows_[CurrentRowIndex_];
    }

    TKey GetCurrentKey() const
    {
        YT_ASSERT(!Exhausted());

        return Keys_[CurrentRowIndex_];
    }

    void Advance()
    {
        YT_ASSERT(!Exhausted());

        ++CurrentRowIndex_;
    }

    bool Exhausted() const
    {
        YT_ASSERT(CurrentRowIndex_ <= std::ssize(Rows_));

        return CurrentRowIndex_ == std::ssize(Rows_);
    }

    bool IsOpened() const
    {
        return Opened_;
    }

    bool Read(const TRowBatchReadOptions& options)
    {
        YT_VERIFY(Exhausted());

        TRowBatchReadOptions adjustedOptions{
            .MaxRowsPerRead = std::max<i64>(16, static_cast<i64>(options.MaxRowsPerRead * Fraction_)),
            .MaxDataWeightPerRead = std::max<i64>(1_KB, static_cast<i64>(options.MaxDataWeightPerRead * Fraction_))
        };

        if (Batch_ = Reader_->Read(adjustedOptions)) {
            Rows_ = Batch_->MaterializeRows();
            Keys_.resize(Rows_.size());
            for (i64 index = 0; index < std::ssize(Rows_); ++index) {
                Keys_[index] = TKey::FromRowUnchecked(Rows_[index], Comparator_.GetLength());
            }

            CurrentRowIndex_ = 0;

            if (!Opened_ && !Rows_.empty()) {
                Opened_ = true;
                TableIndex_ = GetTableIndex(Rows_[0]);
            }

            return true;
        } else {
            Rows_ = {};
            Keys_.clear();
            CurrentRowIndex_ = 0;

            return false;
        }
    }

    i64 GetTableRowIndex() const
    {
        auto firstRowIndex = Reader_->GetTableRowIndex() - std::ssize(Rows_);
        return firstRowIndex + CurrentRowIndex_;
    }

    TFuture<void> GetReadyEvent() const
    {
        return Reader_->GetReadyEvent();
    }

    TInterruptDescriptor GetInterruptDescriptor(i64 unreadRowCount) const
    {
        YT_VERIFY(unreadRowCount <= CurrentRowIndex_);
        auto unreadRows = Rows_.Slice(CurrentRowIndex_ - unreadRowCount, Rows_.size());
        return Reader_->GetInterruptDescriptor(unreadRows);
    }

private:
    const double Fraction_;

    IUnversionedRowBatchPtr Batch_;
    TSharedRange<TUnversionedRow> Rows_;
    std::vector<TKey> Keys_;
    int CurrentRowIndex_ = 0;

    //! Whether at least one successful read was done
    //! and table index is evaluated.
    bool Opened_ = false;

    int GetTableIndex(TUnversionedRow row) const
    {
        int tableIndexId;
        try {
            const auto& nameTable = Reader_->GetNameTable();
            tableIndexId = nameTable->GetIdOrRegisterName(TableIndexColumnName);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to add system column to name table for schemaless merging reader")
                << ex;
        }

        for (const auto& value : row) {
            if (value.Id == tableIndexId) {
                YT_VERIFY(value.Type == EValueType::Int64);
                return value.Data.Int64;
            }
        }

        return 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSortedMergingReaderBase
    : public ISchemalessMultiChunkReader
    , public TTimingReaderBase
{
public:
    explicit TSortedMergingReaderBase(bool interruptAtKeyEdge)
        : InterruptAtKeyEdge_(interruptAtKeyEdge)
    { }

    void SetStreams(std::vector<std::unique_ptr<TSortedStream>> streams)
    {
        Streams_ = std::move(streams);

        for (const auto& stream : Streams_) {
            TotalRowCount_ += stream->Reader()->GetTotalRowCount();
        }
    }

    // IReaderBase implementation.

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics statistics;
        for (const auto& stream : Streams_) {
            statistics += stream->Reader()->GetDataStatistics();
        }

        statistics.set_row_count(FetchedRowCount_);
        statistics.set_data_weight(FetchedDataWeight_);

        return statistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        TCodecStatistics statistics;
        for (const auto& stream : Streams_) {
            statistics += stream->Reader()->GetDecompressionStatistics();
        }

        return statistics;
    }

    bool IsFetchingCompleted() const override
    {
        for (const auto& stream : Streams_) {
            if (!stream->Reader()->IsFetchingCompleted()) {
                return false;
            }
        }

        return true;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        std::vector<TChunkId> failedChunkIds;
        for (const auto& stream : Streams_) {
            auto readerFailedChunkIds = stream->Reader()->GetFailedChunkIds();
            failedChunkIds.insert(
                failedChunkIds.end(),
                readerFailedChunkIds.begin(),
                readerFailedChunkIds.end());
        }

        return failedChunkIds;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Streams_.front()->Reader()->GetNameTable();
    }

    // ISchemalessChunkReader implementation.

    i64 GetTableRowIndex() const override
    {
        return TableRowIndex_;
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

    // ISchemalessMultiChunkReader implementation.

    i64 GetSessionRowIndex() const override
    {
        return FetchedRowCount_;
    }

    i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    void Interrupt() override
    {
        Interrupting_ = true;

        if (!InterruptAtKeyEdge_) {
            FinishShortcutPromise_.TrySet();
        }
    }

    void SkipCurrentReader() override
    {
        // Not supported.
    }

protected:
    std::vector<std::unique_ptr<TSortedStream>> Streams_;
    std::vector<TSortedStream*> StreamHeap_;

    const bool InterruptAtKeyEdge_ = true;

    std::atomic<bool> Interrupting_ = false;

    //! May be set during interrupt to nofify early competion of ready event.
    TPromise<void> FinishShortcutPromise_ = NewPromise<void>();

    //! Amount and cumulative data weight of rows returns via |Read| call.
    i64 FetchedRowCount_ = 0;
    i64 FetchedDataWeight_ = 0;

    //! Total number of rows in all streams minus number of
    //! unjoined foreign rows.
    i64 TotalRowCount_ = 0;

    //! Whether streams are opened and put into the heap.
    bool Opened_ = false;

    i64 TableRowIndex_ = 0;

    void SetReadyEvent(TFuture<void> readyEvent, bool combineWithShortcut = true)
    {
        if (combineWithShortcut) {
            readyEvent = AnySet(
                std::vector{readyEvent, FinishShortcutPromise_.ToFuture()},
                TFutureCombinerOptions{.CancelInputOnShortcut = false});
        }
        TReadyEventReaderBase::SetReadyEvent(readyEvent);
    }

    void Open(const TRowBatchReadOptions& options)
    {
        YT_VERIFY(!Opened_);
        Opened_ = true;

        YT_LOG_DEBUG("Opening sorted merging reader (StreamCount: %v)",
            Streams_.size());

        auto readyEvent = BIND(&TSortedMergingReaderBase::DoOpen, MakeStrong(this), options)
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
        // NB: we don't combine completion error here, because reader opening must not be interrupted.
        // Otherwise, race condition may occur between reading in DoOpen and GetInterruptDescriptor.
        SetReadyEvent(readyEvent, /*combineWithShortcut*/ false);
    }

    void DoOpen(const TRowBatchReadOptions& options)
    {
        try {
            GuardedDoOpen(options);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to open schemaless merging reader")
                << ex;
        }
    }

    void GuardedDoOpen(const TRowBatchReadOptions& options)
    {
        StreamHeap_.reserve(Streams_.size());
        for (const auto& stream : Streams_) {
            while (!stream->IsOpened()) {
                if (!stream->Read(options)) {
                    // Empty stream, do nothing.
                    break;
                }

                if (stream->IsOpened()) {
                    break;
                }

                WaitFor(stream->GetReadyEvent())
                    .ThrowOnError();
            }

            if (stream->IsOpened()) {
                StreamHeap_.push_back(stream.get());
            }
        }

        MakeHeap(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
    }

    bool ArmStream(const TRowBatchReadOptions& options)
    {
        auto* stream = StreamHeap_.front();
        if (stream->Exhausted()) {
            if (stream->Read(options)) {
                if (stream->Exhausted()) {
                    SetReadyEvent(stream->GetReadyEvent());
                } else {
                    AdjustHeapFront(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
                }
            } else {
                ExtractHeap(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
                StreamHeap_.pop_back();
            }
            return false;
        }

        TableRowIndex_ = stream->GetTableRowIndex();

        return true;
    }

    static bool CompareStreams(const TSortedStream* lhs, const TSortedStream* rhs)
    {
        YT_ASSERT(lhs->GetTableIndex() != -1);
        YT_ASSERT(rhs->GetTableIndex() != -1);

        auto lhsKey = lhs->GetCurrentKey();
        auto rhsKey = rhs->GetCurrentKey();

        auto comparisonResult = lhs->Comparator().CompareKeys(lhsKey, rhsKey);
        if (comparisonResult != 0) {
            return comparisonResult < 0;
        }

        return lhs->GetTableIndex() < rhs->GetTableIndex();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSortedMergingReader
    : public TSortedMergingReaderBase
{
public:
    TSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        TComparator sortComparator,
        TComparator mergeComparator,
        bool interruptAtKeyEdge)
        : TSortedMergingReaderBase(interruptAtKeyEdge)
        , MergeComparator_(std::move(mergeComparator))
    {
        std::vector<std::unique_ptr<TSortedStream>> streams;
        streams.reserve(readers.size());
        for (int index = 0; index < std::ssize(readers); ++index) {
            streams.push_back(std::make_unique<TSortedStream>(
                readers[index],
                sortComparator,
                index,
                /*fraction*/ 1.0 / std::ssize(readers)));
        }
        SetStreams(std::move(streams));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        LastReadRowStreamIndexes_.clear();
        LastReadRowStreamIndexes_.reserve(options.MaxRowsPerRead);

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (!Opened_) {
            Open(options);
            return CreateEmptyUnversionedRowBatch();
        }

        if (StreamHeap_.empty()) {
            return nullptr;
        }

        if (!ArmStream(options)) {
            return CreateEmptyUnversionedRowBatch();
        }

        auto* stream = StreamHeap_.front();
        YT_VERIFY(!stream->Exhausted());

        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        i64 dataWeight = 0;
        auto interrupting = Interrupting_.load();

        auto lastKey = LastKey_;

        while (
            std::ssize(rows) < options.MaxRowsPerRead &&
            dataWeight < options.MaxDataWeightPerRead)
        {
            auto row = stream->GetCurrentRow();
            auto key = TKey::FromRowUnchecked(row, MergeComparator_.GetLength());

            auto canInterrupt = false;
            if (interrupting) {
                if (InterruptAtKeyEdge_) {
                    canInterrupt = (!lastKey || MergeComparator_.CompareKeys(lastKey, key) < 0);
                } else if (!InterruptAtKeyEdge_) {
                    canInterrupt = true;
                }
            }

            if (canInterrupt) {
                YT_LOG_DEBUG("Sorted merging reader interrupted (LastKey: %v, Key: %v)",
                    lastKey,
                    key);

                SetReadyEvent(VoidFuture);
                StreamHeap_.clear();
                return rows.empty()
                    ? nullptr
                    : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
            }

            rows.push_back(row);
            stream->Advance();

            dataWeight += GetDataWeight(row);

            LastReadRowStreamIndexes_.push_back(stream->GetStreamIndex());

            lastKey = key;

            if (stream->Exhausted()) {
                YT_VERIFY(!rows.empty());
                break;
            }

            AdjustHeapFront(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
            stream = StreamHeap_.front();
        }

        FetchedRowCount_ += std::ssize(rows);
        FetchedDataWeight_ += dataWeight;

        if (lastKey) {
            LastKeyHolder_ = TUnversionedOwningRow({lastKey.Begin(), lastKey.End()});
            LastKey_ = TKey::FromRowUnchecked(LastKeyHolder_, MergeComparator_.GetLength());
        }

        TableRowIndex_ = stream->GetTableRowIndex();

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override
    {
        YT_LOG_DEBUG("Creating interrupt descriptor for sorted merging reader (UnreadRowCount: %v)",
            unreadRows.Size());

        TInterruptDescriptor descriptor;

        std::vector<i64> unreadRowCounts(Streams_.size(), 0);

        YT_VERIFY(unreadRows.size() <= LastReadRowStreamIndexes_.size());
        for (int index = LastReadRowStreamIndexes_.size() - unreadRows.size();
            index < std::ssize(LastReadRowStreamIndexes_);
            ++index)
        {
            ++unreadRowCounts[LastReadRowStreamIndexes_[index]];
        }

        for (const auto& stream : Streams_) {
            auto unreadRowCount = unreadRowCounts[stream->GetStreamIndex()];
            descriptor.MergeFrom(stream->GetInterruptDescriptor(unreadRowCount));
        }

        return descriptor;
    }

private:
    const TComparator MergeComparator_;

    std::vector<int> LastReadRowStreamIndexes_;

    TUnversionedOwningRow LastKeyHolder_;
    TKey LastKey_;
};

////////////////////////////////////////////////////////////////////////////////

class TSortedJoiningReader
    : public TSortedMergingReaderBase
{
public:
    TSortedJoiningReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
        TComparator sortComparator,
        TComparator mergeComparator,
        const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
        TComparator joinComparator,
        bool interruptAtKeyEdge)
        : TSortedMergingReaderBase(interruptAtKeyEdge)
        , MergeComparator_(std::move(mergeComparator))
        , JoinComparator_(std::move(joinComparator))
    {
        std::vector<std::unique_ptr<TSortedStream>> streams;
        streams.reserve(foreignReaders.size() + 1);

        auto mergingReader = CreateSortedMergingReader(
            primaryReaders,
            sortComparator,
            MergeComparator_,
            interruptAtKeyEdge);
        auto primaryStream = std::make_unique<TSortedStream>(
            std::move(mergingReader),
            JoinComparator_,
            /*sessionIndex*/ 0,
            /*fraction*/ 0.5);
        PrimaryStream_ = primaryStream.get();
        streams.push_back(std::move(primaryStream));
        for (const auto& reader : foreignReaders) {
            auto sessionIndex = std::ssize(streams);
            streams.push_back(std::make_unique<TSortedStream>(
                reader,
                JoinComparator_,
                sessionIndex,
                0.5 / std::ssize(foreignReaders)));
        }
        SetStreams(std::move(streams));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (!Opened_) {
            Open(options);
            return CreateEmptyUnversionedRowBatch();
        }

        if (StreamHeap_.empty()) {
            return nullptr;
        }

        if (!ArmStream(options)) {
            return CreateEmptyUnversionedRowBatch();
        }

        auto* stream = StreamHeap_.front();
        YT_VERIFY(!stream->Exhausted());

        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        i64 dataWeight = 0;
        auto interrupting = Interrupting_.load();

        auto lastPrimaryKey = LastPrimaryKey_;

        while (
            std::ssize(rows) < options.MaxRowsPerRead &&
            dataWeight < options.MaxDataWeightPerRead)
        {
            auto row = stream->GetCurrentRow();

            if (interrupting) {
                // Whether current reader has completed reading of all the keys before #lastKey (inclusive)
                // and thus can be removed from the session heap.
                bool extractCurrentSession = false;
                if (stream == PrimaryStream_) {
                    // If |interruptAtKeyEdge| is true, primary reader can be extracted from heap at key switch only.
                    auto key = TKey::FromRowUnchecked(row, MergeComparator_.GetLength());
                    if (InterruptAtKeyEdge_ && lastPrimaryKey) {
                        int comparisonResult = MergeComparator_.CompareKeys(lastPrimaryKey, key);
                        YT_VERIFY(comparisonResult <= 0);
                        if (comparisonResult < 0) {
                            extractCurrentSession = true;
                        }
                    } else {
                        extractCurrentSession = true;
                    }

                    YT_LOG_DEBUG("Extracting primary stream from heap due to interrupt (Key: %v, LastKev: %v, TableIndex: %v)",
                        key,
                        lastPrimaryKey,
                        stream->GetTableIndex());
                } else {
                    // Regardless of #interruptAtKeyEdge we have to read all the foreign rows
                    // corresponding to #lastKey.
                    auto key = TKey::FromRow(row, JoinComparator_.GetLength());
                    if (lastPrimaryKey) {
                        TKey lastKey({lastPrimaryKey.Begin(), lastPrimaryKey.Begin() + JoinComparator_.GetLength()});
                        int comparisonResult = JoinComparator_.CompareKeys(lastKey, key);
                        if (comparisonResult < 0) {
                            extractCurrentSession = true;
                        }
                    } else {
                        extractCurrentSession = true;
                    }

                    YT_LOG_DEBUG("Extracting foreign session from heap due to interrupt (Key: %v, LastKey: %v, TableIndex: %v)",
                        key,
                        lastPrimaryKey,
                        stream->GetTableIndex());
                }

                if (extractCurrentSession) {
                    ExtractHeap(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
                    YT_VERIFY(StreamHeap_.back() == stream);
                    StreamHeap_.pop_back();

                    if (!rows.empty()) {
                        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
                    } else if (StreamHeap_.empty()) {
                        return nullptr;
                    } else {
                        return CreateEmptyUnversionedRowBatch();
                    }
                }
            }

            auto outputCurrentRow = false;
            if (stream == PrimaryStream_) {
                lastPrimaryKey = TKey::FromRowUnchecked(row, MergeComparator_.GetLength());
                outputCurrentRow = true;
            } else {
                // Foreign row should be output iff it equals to last primary key consumed or next primary key.
                auto foreignKey = TKey::FromRow(row, JoinComparator_.GetLength());
                if (lastPrimaryKey) {
                    TKey primaryKey({lastPrimaryKey.Begin(), lastPrimaryKey.Begin() + JoinComparator_.GetLength()});
                    if (JoinComparator_.CompareKeys(primaryKey, foreignKey) == 0) {
                        outputCurrentRow = true;
                    }
                }
                // If reader is interrupting, no new primary keys will occur.
                if (!outputCurrentRow && !PrimaryStream_->Exhausted() && !interrupting) {
                    auto nextKey = TKey::FromRow(PrimaryStream_->GetCurrentRow(), JoinComparator_.GetLength());
                    if (JoinComparator_.CompareKeys(nextKey, foreignKey) == 0) {
                        outputCurrentRow = true;
                    }
                }
            }

            if (outputCurrentRow) {
                rows.push_back(row);
                dataWeight += GetDataWeight(row);
            } else {
                --TotalRowCount_;
            }

            stream->Advance();

            if (stream->Exhausted()) {
                break;
            }

            AdjustHeapFront(StreamHeap_.begin(), StreamHeap_.end(), CompareStreams);
            stream = StreamHeap_.front();
        }

        FetchedRowCount_ += std::ssize(rows);
        FetchedDataWeight_ += dataWeight;

        if (lastPrimaryKey) {
            LastPrimaryKeyHolder_ = TUnversionedOwningRow({lastPrimaryKey.Begin(), lastPrimaryKey.End()});
            LastPrimaryKey_ = TKey::FromRowUnchecked(LastPrimaryKeyHolder_, MergeComparator_.GetLength());
        }

        TableRowIndex_ = stream->GetTableRowIndex();

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override
    {
        YT_VERIFY(unreadRows.Empty());

        // Returns only UnreadDataSlices from primary readers.
        return PrimaryStream_->GetInterruptDescriptor(/*unreadRowCount*/ 0);
    }

private:
    const TComparator MergeComparator_;
    const TComparator JoinComparator_;

    const TSortedStream* PrimaryStream_;

    TUnversionedOwningRow LastPrimaryKeyHolder_;
    TKey LastPrimaryKey_;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    TComparator sortComparator,
    TComparator mergeComparator,
    bool interruptAtKeyEdge)
{
    YT_VERIFY(!readers.empty());
    // The only input reader can not satisfy key guarantee.
    if (readers.size() == 1 && !interruptAtKeyEdge) {
        return readers[0];
    } else {
        return New<TSortedMergingReader>(
            readers,
            std::move(sortComparator),
            std::move(mergeComparator),
            interruptAtKeyEdge);
    }
}

ISchemalessMultiChunkReaderPtr CreateSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    TComparator primaryComparator,
    TComparator mergeComparator,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    TComparator joinComparator,
    bool interruptAtKeyEdge)
{
    YT_VERIFY(!primaryReaders.empty());
    YT_VERIFY(primaryComparator.GetLength() >= mergeComparator.GetLength());
    YT_VERIFY(mergeComparator.GetLength() >= joinComparator.GetLength());
    if (foreignReaders.empty()) {
        return CreateSortedMergingReader(
            primaryReaders,
            std::move(primaryComparator),
            std::move(mergeComparator),
            interruptAtKeyEdge);
    } else {
        return New<TSortedJoiningReader>(
            primaryReaders,
            std::move(primaryComparator),
            std::move(mergeComparator),
            foreignReaders,
            std::move(joinComparator),
            interruptAtKeyEdge);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
