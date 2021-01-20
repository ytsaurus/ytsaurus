#include "sorted_merging_reader.h"

#include "private.h"
#include "schemaless_multi_chunk_reader.h"
#include "timing_reader.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_batch.h>

#include <yt/core/misc/heap.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSortedMergingReaderBase
    : public ISchemalessMultiChunkReader
    , public TTimingReaderBase
{
public:
    TSortedMergingReaderBase(
        TComparator sortComparator,
        TComparator reduceComparator,
        bool interruptAtKeyEdge);

    virtual TDataStatistics GetDataStatistics() const override;

    virtual TCodecStatistics GetDecompressionStatistics() const override;

    virtual bool IsFetchingCompleted() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

    virtual const TNameTablePtr& GetNameTable() const override;

    virtual i64 GetTotalRowCount() const override;
    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetTableRowIndex() const override;

    virtual void Interrupt() override;

    virtual void SkipCurrentReader() override;

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

protected:
    struct TSession
    {
        TSession(ISchemalessMultiChunkReaderPtr reader, int sessionIndex, double fraction = 1.0)
            : Reader(std::move(reader))
            , SessionIndex(sessionIndex)
            , Fraction(fraction)
        { }

        TUnversionedRow GetCurrentRow() const
        {
            return Rows[CurrentRowIndex];
        }

        TKey GetCurrentKey(int keyColumnCount) const
        {
            return TKey::FromRow(GetCurrentRow(), keyColumnCount);
        }

        bool Exhausted() const
        {
            return CurrentRowIndex == Rows.size();
        }

        i64 GetTableRowIndex() const
        {
            i64 firstRowIndex = Reader->GetTableRowIndex() - Rows.size();
            return firstRowIndex + CurrentRowIndex;
        }

        bool Populate(const TRowBatchReadOptions& options)
        {
            YT_VERIFY(Exhausted());

            CurrentRowIndex = 0;

            TRowBatchReadOptions adjustedOptions{
                .MaxRowsPerRead = std::max<i64>(16, static_cast<i64>(Fraction * options.MaxRowsPerRead)),
                .MaxDataWeightPerRead = std::max<i64>(1_KB, static_cast<i64>(Fraction * options.MaxDataWeightPerRead))
            };

            if (Batch = Reader->Read(adjustedOptions)) {
                Rows = Batch->MaterializeRows();
                return true;
            } else {
                Rows = {};
                return false;
            }
        }

        const ISchemalessMultiChunkReaderPtr Reader;
        const int SessionIndex;
        const double Fraction;

        IUnversionedRowBatchPtr Batch;
        TSharedRange<TUnversionedRow> Rows;
        int CurrentRowIndex = 0;
        int TableIndex = 0;
    };

    const TComparator SortComparator_;
    const TComparator ReduceComparator_;
    const bool InterruptAtKeyEdge_;

    std::vector<TSession> SessionHolder_;
    std::vector<TSession*> SessionHeap_;

    i64 RowCount_ = 0;
    i64 RowIndex_ = 0;
    i64 DataWeight_ = 0;

    TPromise<void> CompletionError_ = NewPromise<void>();
    i64 TableRowIndex_ = 0;

    std::atomic<bool> Interrupting_ = false;

    std::function<bool(const TSession* lhs, const TSession* rhs)> CompareSessions_;

    bool EnsureOpen(const TRowBatchReadOptions& options);
    TFuture<void> CombineCompletionError(TFuture<void> future);

private:
    bool Open_ = false;

    void DoOpen(const TRowBatchReadOptions& options);
};

////////////////////////////////////////////////////////////////////////////////

TSortedMergingReaderBase::TSortedMergingReaderBase(
    TComparator sortComparator,
    TComparator reduceComparator,
    bool interruptAtKeyEdge)
    : SortComparator_(std::move(sortComparator))
    , ReduceComparator_(std::move(reduceComparator))
    , InterruptAtKeyEdge_(interruptAtKeyEdge)
{
    CompareSessions_ = [=] (const TSession* lhs, const TSession* rhs) {
        auto lhsKey = lhs->GetCurrentKey(SortComparator_.GetLength());
        auto rhsKey = rhs->GetCurrentKey(SortComparator_.GetLength());

        auto comparisonResult = SortComparator_.CompareKeys(lhsKey, rhsKey);

        if (comparisonResult == 0) {
            return lhs->TableIndex < rhs->TableIndex;
        }
        return comparisonResult < 0;
    };
}

bool TSortedMergingReaderBase::EnsureOpen(const TRowBatchReadOptions& options)
{
    if (Open_) {
        return true;
    }

    YT_LOG_INFO("Opening schemaless sorted merging reader (SessionCount: %v)",
        SessionHolder_.size());

    // NB: we don't combine completion error here, because reader opening must not be interrupted.
    // Otherwise, race condition may occur between reading in DoOpen and GetInterruptDescriptor.
    SetReadyEvent(BIND(&TSortedMergingReaderBase::DoOpen, MakeStrong(this), options)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run());

    Open_ = true;
    return false;
}

void TSortedMergingReaderBase::DoOpen(const TRowBatchReadOptions& options)
{
    auto getTableIndex = [] (TUnversionedRow row, TNameTablePtr nameTable) -> int {
        int tableIndexId;
        try {
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
    };

    try {
        for (auto& session : SessionHolder_) {
            while (true) {
                if (!session.Populate(options)) {
                    break;
                }

                session.Rows = session.Batch->MaterializeRows();
                if (!session.Rows.Empty()) {
                    session.TableIndex = getTableIndex(session.Rows[0], session.Reader->GetNameTable());
                    SessionHeap_.push_back(&session);
                    break;
                }

                WaitFor(session.Reader->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        if (!SessionHeap_.empty()) {
            MakeHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to open schemaless merging reader") << ex;
    }
}

const TDataSliceDescriptor& TSortedMergingReaderBase::GetCurrentReaderDescriptor() const
{
    YT_ABORT();
}

TDataStatistics TSortedMergingReaderBase::GetDataStatistics() const
{
    TDataStatistics dataStatistics;

    for (const auto& session : SessionHolder_) {
        dataStatistics += session.Reader->GetDataStatistics();
    }
    dataStatistics.set_row_count(RowIndex_);
    dataStatistics.set_data_weight(DataWeight_);

    return dataStatistics;
}

TCodecStatistics TSortedMergingReaderBase::GetDecompressionStatistics() const
{
    TCodecStatistics result;
    for (const auto& session : SessionHolder_) {
        result += session.Reader->GetDecompressionStatistics();
    }
    return result;
}

bool TSortedMergingReaderBase::IsFetchingCompleted() const
{
    return std::all_of(
        SessionHolder_.begin(),
        SessionHolder_.end(),
        [] (const TSession& session) {
            return session.Reader->IsFetchingCompleted();
        });
}

std::vector<TChunkId> TSortedMergingReaderBase::GetFailedChunkIds() const
{
    std::vector<TChunkId> result;
    for (const auto& session : SessionHolder_) {
        auto failedChunks = session.Reader->GetFailedChunkIds();
        result.insert(result.end(), failedChunks.begin(), failedChunks.end());
    }
    return result;
}

void TSortedMergingReaderBase::Interrupt()
{
    Interrupting_ = true;

    // Return ready event to consumer if we don't wait for key edge.
    if (!InterruptAtKeyEdge_) {
        CompletionError_.TrySet();
    }
}

void TSortedMergingReaderBase::SkipCurrentReader()
{
    // Sorted merging reader doesn't support sub-reader skipping.
}

const TNameTablePtr& TSortedMergingReaderBase::GetNameTable() const
{
    return SessionHolder_.front().Reader->GetNameTable();
}

i64 TSortedMergingReaderBase::GetTotalRowCount() const
{
    return RowCount_;
}

i64 TSortedMergingReaderBase::GetSessionRowIndex() const
{
    return RowIndex_;
}

i64 TSortedMergingReaderBase::GetTableRowIndex() const
{
    return TableRowIndex_;
}

TFuture<void> TSortedMergingReaderBase::CombineCompletionError(TFuture<void> future)
{
    return AnySet(
        std::vector{std::move(future), CompletionError_.ToFuture()},
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
}

////////////////////////////////////////////////////////////////////////////////

class TSortedMergingReader
    : public TSortedMergingReaderBase
{
public:
    TSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        TComparator sortComparator,
        TComparator reduceComparator,
        bool interruptAtKeyEdge);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

private:
    TUnversionedOwningRow LastKeyHolder_;
    std::optional<TKey> LastKey_;

    std::vector<int> LastReadRowSessionIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

TSortedMergingReader::TSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    TComparator sortComparator,
    TComparator reduceComparator,
    bool interruptAtKeyEdge)
    : TSortedMergingReaderBase(
        std::move(sortComparator),
        std::move(reduceComparator),
        interruptAtKeyEdge)
{
    YT_VERIFY(!readers.empty());

    SessionHolder_.reserve(readers.size());
    SessionHeap_.reserve(readers.size());

    for (const auto& reader : readers) {
        SessionHolder_.emplace_back(reader, SessionHolder_.size(), 1.0 / readers.size());
        RowCount_ += reader->GetTotalRowCount();
    }
}

IUnversionedRowBatchPtr TSortedMergingReader::Read(const TRowBatchReadOptions& options)
{
    LastReadRowSessionIndexes_.clear();

    if (!EnsureOpen(options) || !ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    auto* session = SessionHeap_.front();
    if (session->Exhausted()) {
        if (session->Populate(options)) {
            if (session->Rows.Empty()) {
                SetReadyEvent(CombineCompletionError(session->Reader->GetReadyEvent()));
            } else {
                AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
            }
        } else {
            ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
            SessionHeap_.pop_back();
        }
        return CreateEmptyUnversionedRowBatch();
    }

    TableRowIndex_ = session->GetTableRowIndex();

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;
    bool interrupting = Interrupting_;
    while (rows.size() < options.MaxRowsPerRead &&
        dataWeight < options.MaxDataWeightPerRead)
    {
        auto key = session->GetCurrentKey(ReduceComparator_.GetLength());

        if (interrupting) {
            // If #interruptAtKeyEdge is true, reader can be interrupted only at key switch.
            // If #interruptAtKeyEdge is false, reader can be interrupted everywhere.
            bool canInterrupt = true;
            if (InterruptAtKeyEdge_ && LastKey_) {
                auto comparisonResult = ReduceComparator_.CompareKeys(*LastKey_, key);
                YT_VERIFY(comparisonResult <= 0);
                canInterrupt = (comparisonResult < 0);
            }

            if (canInterrupt) {
                YT_LOG_DEBUG("Sorted merging reader interrupted (LastKey: %v, Key: %v)",
                    LastKey_,
                    key);

                SetReadyEvent(VoidFuture);
                SessionHeap_.clear();
                return rows.empty()
                    ? nullptr
                    : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
            }
        }

        auto row = session->GetCurrentRow();
        rows.push_back(row);
        LastReadRowSessionIndexes_.push_back(session->SessionIndex);
        dataWeight += GetDataWeight(row);
        ++session->CurrentRowIndex;
        ++TableRowIndex_;
        ++RowIndex_;

        if (session->CurrentRowIndex == session->Rows.size()) {
            // Out of prefetched rows in this session.
            break;
        }

        AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);

        if (SessionHeap_.front() != session) {
            session = SessionHeap_.front();
            TableRowIndex_ = session->GetTableRowIndex();
        }
    }

    if (!rows.empty()) {
        LastKeyHolder_ = TUnversionedOwningRow(rows.back());
        LastKey_ = TKey::FromRow(LastKeyHolder_, ReduceComparator_.GetLength());
    }

    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TInterruptDescriptor TSortedMergingReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    YT_LOG_DEBUG("Creating interrupt descriptor for sorted merging reader (UnreadRowCount: %v)",
        unreadRows.Size());

    TInterruptDescriptor result;

    std::vector<i64> unreadRowCounts(SessionHolder_.size(), 0);

    YT_VERIFY(unreadRows.size() <= LastReadRowSessionIndexes_.size());
    for (int index = LastReadRowSessionIndexes_.size() - unreadRows.size(); index < LastReadRowSessionIndexes_.size(); ++index) {
        ++unreadRowCounts[LastReadRowSessionIndexes_[index]];
    }

    for (const auto& session : SessionHolder_) {
        auto unreadRowCount = unreadRowCounts[session.SessionIndex];
        YT_VERIFY(unreadRowCount <= session.CurrentRowIndex);

        auto interruptDescriptor = session.Reader->GetInterruptDescriptor(
            session.Rows.Slice(
                session.CurrentRowIndex - unreadRowCount,
                session.Rows.Size()));

        result.MergeFrom(std::move(interruptDescriptor));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TSortedJoiningReader
    : public TSortedMergingReaderBase
{
public:
    TSortedJoiningReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
        TComparator primaryComparator,
        TComparator reduceComparator,
        const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
        TComparator foreignComparator,
        bool interruptAtKeyEdge);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

private:
    const bool InterruptAtKeyEdge_;

    const TComparator ReduceComparator_;
    const TComparator ForeignComparator_;

    TSession* PrimarySession_;

    TUnversionedOwningRow LastKeyHolder_;
    std::optional<TKey> LastKey_;
};

////////////////////////////////////////////////////////////////////////////////

TSortedJoiningReader::TSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    TComparator primaryComparator,
    TComparator reduceComparator,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    TComparator foreignComparator,
    bool interruptAtKeyEdge)
    : TSortedMergingReaderBase(
        foreignComparator,
        reduceComparator,
        interruptAtKeyEdge)
    , InterruptAtKeyEdge_(interruptAtKeyEdge)
    , ReduceComparator_(reduceComparator)
    , ForeignComparator_(foreignComparator)
{
    YT_VERIFY(!primaryReaders.empty() && !foreignReaders.empty());

    auto mergingReader = CreateSortedMergingReader(
        primaryReaders,
        primaryComparator,
        reduceComparator,
        InterruptAtKeyEdge_);

    SessionHolder_.reserve(foreignReaders.size() + 1);
    SessionHeap_.reserve(foreignReaders.size() + 1);

    SessionHolder_.emplace_back(mergingReader, SessionHolder_.size(), 0.5);
    PrimarySession_ = &SessionHolder_[0];

    RowCount_ = mergingReader->GetTotalRowCount();
    for (const auto& reader : foreignReaders) {
        SessionHolder_.emplace_back(reader, SessionHolder_.size(), 0.5 / foreignReaders.size());
        RowCount_ += reader->GetTotalRowCount();
    }
}

IUnversionedRowBatchPtr TSortedJoiningReader::Read(const TRowBatchReadOptions& options)
{
    if (!EnsureOpen(options) || !ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    bool interrupting = Interrupting_;

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    auto* session = SessionHeap_.front();
    if (session->Exhausted()) {
        if (session->Populate(options)) {
            if (session->Rows.Empty()) {
                SetReadyEvent(CombineCompletionError(session->Reader->GetReadyEvent()));
            } else {
                AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
            }
        } else {
            ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
            SessionHeap_.pop_back();
        }
        return CreateEmptyUnversionedRowBatch();
    }

    TableRowIndex_ = session->GetTableRowIndex();

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    while (rows.size() < options.MaxRowsPerRead &&
        dataWeight < options.MaxDataWeightPerRead)
    {
        auto row = session->Rows[session->CurrentRowIndex];
        if (interrupting) {
            // Whether current reader has completed reading of all the keys before #lastKey (inclusive)
            // and thus can be removed from the session heap.
            bool extractCurrentSession = false;
            if (session == PrimarySession_) {
                // If #interruptAtKeyEdge is true, primary reader can be extracted from heap at key switch only.
                auto key = TKey::FromRow(row, ReduceComparator_.GetLength());
                if (InterruptAtKeyEdge_ && LastKey_) {
                    int comparisonResult = ReduceComparator_.CompareKeys(*LastKey_, key);
                    YT_VERIFY(comparisonResult <= 0);
                    if (comparisonResult < 0) {
                        extractCurrentSession = true;
                    }
                } else {
                    extractCurrentSession = true;
                }

                YT_LOG_DEBUG("Extracting primary session from heap due to interrupt (Key: %v, LastKev: %v, TableIndex: %v)",
                    key,
                    LastKey_,
                    session->TableIndex);
            } else {
                // Regardless of #interruptAtKeyEdge we have to read all the foreign rows
                // corresponding to #lastKey.
                auto key = TKey::FromRow(row, ForeignComparator_.GetLength());
                if (LastKey_) {
                    auto lastKey = TKey::FromRow(LastKeyHolder_, ForeignComparator_.GetLength());
                    int comparisonResult = ForeignComparator_.CompareKeys(lastKey, key);
                    if (comparisonResult < 0) {
                        extractCurrentSession = true;
                    }
                } else {
                    extractCurrentSession = true;
                }

                YT_LOG_DEBUG("Extracting foreign session from heap due to interrupt (Key: %v, LastKey: %v, TableIndex: %v)",
                    key,
                    LastKey_,
                    session->TableIndex);
            }

            if (extractCurrentSession) {
                ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
                YT_VERIFY(SessionHeap_.back() == session);
                SessionHeap_.pop_back();

                if (!rows.empty()) {
                    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
                } else if (SessionHeap_.empty()) {
                    return nullptr;
                } else {
                    return CreateEmptyUnversionedRowBatch();
                }
            }
        }

        bool outputCurrentRow = false;
        if (session == PrimarySession_) {
            LastKeyHolder_ = TUnversionedOwningRow(row);
            LastKey_ = TKey::FromRow(LastKeyHolder_, ReduceComparator_.GetLength());
            outputCurrentRow = true;
        } else {
            // Foreign row should be output iff it equals to last primary key consumed or next primary key.
            auto foreignKey = TKey::FromRow(row, ForeignComparator_.GetLength());
            if (LastKey_) {
                auto primaryKey = TKey::FromRow(LastKeyHolder_, ForeignComparator_.GetLength());
                if (ForeignComparator_.CompareKeys(primaryKey, foreignKey) == 0) {
                    outputCurrentRow = true;
                }
            }
            // If reader is interrupting, no new primary keys will occur.
            if (!outputCurrentRow && !PrimarySession_->Exhausted() && !interrupting) {
                auto nextKey = TKey::FromRow(PrimarySession_->GetCurrentRow(), ForeignComparator_.GetLength());
                if (ForeignComparator_.CompareKeys(nextKey, foreignKey) == 0) {
                    outputCurrentRow = true;
                }
            }
        }

        if (outputCurrentRow) {
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
            ++RowIndex_;
        } else {
            --RowCount_;
        }

        ++session->CurrentRowIndex;
        ++TableRowIndex_;

        if (session->Exhausted()) {
            // Out of prefetched rows in this session.
            break;
        }

        AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);

        if (SessionHeap_.front() != session) {
            session = SessionHeap_.front();
            TableRowIndex_ = session->GetTableRowIndex();
        }
    }

    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TInterruptDescriptor TSortedJoiningReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    YT_VERIFY(unreadRows.Empty());

    // Returns only UnreadDataSlices from primary readers.
    return PrimarySession_->Reader->GetInterruptDescriptor(
        PrimarySession_->Rows.Slice(
            PrimarySession_->CurrentRowIndex,
            PrimarySession_->Rows.Size()));
}

const TDataSliceDescriptor& TSortedJoiningReader::GetCurrentReaderDescriptor() const
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    TComparator sortComparator,
    TComparator reduceComparator,
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
            std::move(reduceComparator),
            interruptAtKeyEdge);
    }
}

ISchemalessMultiChunkReaderPtr CreateSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    TComparator primaryComparator,
    TComparator reduceComparator,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    TComparator foreignComparator,
    bool interruptAtKeyEdge)
{
    YT_VERIFY(!primaryReaders.empty());
    YT_VERIFY(primaryComparator.GetLength() >= reduceComparator.GetLength());
    YT_VERIFY(reduceComparator.GetLength() >= foreignComparator.GetLength());
    if (foreignReaders.empty()) {
        return CreateSortedMergingReader(
            primaryReaders,
            std::move(primaryComparator),
            std::move(reduceComparator),
            interruptAtKeyEdge);
    } else {
        return New<TSortedJoiningReader>(
            primaryReaders,
            std::move(primaryComparator),
            std::move(reduceComparator),
            foreignReaders,
            std::move(foreignComparator),
            interruptAtKeyEdge);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
