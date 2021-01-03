#include "schemaless_sorted_merging_reader.h"

#include "private.h"
#include "schemaless_multi_chunk_reader.h"
#include "timing_reader.h"

#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/client/chunk_client/data_statistics.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/unversioned_row_batch.h>

#include <yt/core/misc/heap.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortedMergingReaderBase
    : public ISchemalessMultiChunkReader
    , public TTimingReaderBase
{
public:
    TSchemalessSortedMergingReaderBase(
        int sortKeyColumnCount,
        int reduceKeyColumnCount,
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

        const ISchemalessMultiChunkReaderPtr Reader;
        const int SessionIndex;
        const double Fraction;

        IUnversionedRowBatchPtr Batch;
        TSharedRange<TUnversionedRow> Rows;
        int CurrentRowIndex = 0;
        int TableIndex = 0;
    };

    const int SortKeyColumnCount_;
    const int ReduceKeyColumnCount_;
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
    bool ReadSession(TSession* session, const TRowBatchReadOptions& options);

private:
    bool Open_ = false;

    void DoOpen(const TRowBatchReadOptions& options);

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessSortedMergingReaderBase::TSchemalessSortedMergingReaderBase(
    int sortKeyColumnCount,
    int reduceKeyColumnCount,
    bool interruptAtKeyEdge)
    : SortKeyColumnCount_(sortKeyColumnCount)
    , ReduceKeyColumnCount_(reduceKeyColumnCount)
    , InterruptAtKeyEdge_(interruptAtKeyEdge)
{
    CompareSessions_ = [=] (const TSession* lhs, const TSession* rhs) {
        int result = CompareRows(
            lhs->Rows[lhs->CurrentRowIndex],
            rhs->Rows[rhs->CurrentRowIndex],
            SortKeyColumnCount_);
        if (result == 0) {
            result = lhs->TableIndex - rhs->TableIndex;
        }
        return result < 0;
    };
}

bool TSchemalessSortedMergingReaderBase::EnsureOpen(const TRowBatchReadOptions& options)
{
    if (Open_) {
        return true;
    }

    YT_LOG_INFO("Opening schemaless sorted merging reader (SessionCount: %v)",
        SessionHolder_.size());

    // NB: we don't combine completion error here, because reader opening must not be interrupted.
    // Otherwise, race condition may occur between reading in DoOpen and GetInterruptDescriptor.
    SetReadyEvent(BIND(&TSchemalessSortedMergingReaderBase::DoOpen, MakeStrong(this), options)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run());

    Open_ = true;
    return false;
}

void TSchemalessSortedMergingReaderBase::DoOpen(const TRowBatchReadOptions& options)
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
                if (!ReadSession(&session, options)) {
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

const TDataSliceDescriptor& TSchemalessSortedMergingReaderBase::GetCurrentReaderDescriptor() const
{
    YT_ABORT();
}

TDataStatistics TSchemalessSortedMergingReaderBase::GetDataStatistics() const
{
    TDataStatistics dataStatistics;

    for (const auto& session : SessionHolder_) {
        dataStatistics += session.Reader->GetDataStatistics();
    }
    dataStatistics.set_row_count(RowIndex_);
    dataStatistics.set_data_weight(DataWeight_);

    return dataStatistics;
}

TCodecStatistics TSchemalessSortedMergingReaderBase::GetDecompressionStatistics() const
{
    TCodecStatistics result;
    for (const auto& session : SessionHolder_) {
        result += session.Reader->GetDecompressionStatistics();
    }
    return result;
}

bool TSchemalessSortedMergingReaderBase::IsFetchingCompleted() const
{
    return std::all_of(
        SessionHolder_.begin(),
        SessionHolder_.end(),
        [] (const TSession& session) {
            return session.Reader->IsFetchingCompleted();
        });
}

std::vector<TChunkId> TSchemalessSortedMergingReaderBase::GetFailedChunkIds() const
{
    std::vector<TChunkId> result;
    for (const auto& session : SessionHolder_) {
        auto failedChunks = session.Reader->GetFailedChunkIds();
        result.insert(result.end(), failedChunks.begin(), failedChunks.end());
    }
    return result;
}

void TSchemalessSortedMergingReaderBase::Interrupt()
{
    Interrupting_ = true;

    // Return ready event to consumer if we don't wait for key edge.
    if (!InterruptAtKeyEdge_) {
        CompletionError_.TrySet();
    }
}

void TSchemalessSortedMergingReaderBase::SkipCurrentReader()
{
    // Sorted merging reader doesn't support sub-reader skipping.
}

const TNameTablePtr& TSchemalessSortedMergingReaderBase::GetNameTable() const
{
    return SessionHolder_.front().Reader->GetNameTable();
}

i64 TSchemalessSortedMergingReaderBase::GetTotalRowCount() const
{
    return RowCount_;
}

i64 TSchemalessSortedMergingReaderBase::GetSessionRowIndex() const
{
    return RowIndex_;
}

i64 TSchemalessSortedMergingReaderBase::GetTableRowIndex() const
{
    return TableRowIndex_;
}

TFuture<void> TSchemalessSortedMergingReaderBase::CombineCompletionError(TFuture<void> future)
{
    return AnySet(
        std::vector{std::move(future), CompletionError_.ToFuture()},
        TFutureCombinerOptions{.CancelInputOnShortcut = false});
}

bool TSchemalessSortedMergingReaderBase::ReadSession(
    TSession* session,
    const TRowBatchReadOptions& options)
{
    TRowBatchReadOptions adjustedOptions{
        .MaxRowsPerRead = std::max<i64>(16, static_cast<i64>(session->Fraction * options.MaxRowsPerRead)),
        .MaxDataWeightPerRead = std::max<i64>(1_KB, static_cast<i64>(session->Fraction * options.MaxDataWeightPerRead))
    };

    if (session->Batch = session->Reader->Read(adjustedOptions)) {
        session->Rows = session->Batch->MaterializeRows();
        return true;
    } else {
        session->Rows = {};
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortedMergingReader
    : public TSchemalessSortedMergingReaderBase
{
public:
    TSchemalessSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        int sortKeyColumnCount,
        int reduceKeyColumnCount,
        bool interruptAtKeyEdge);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

private:
    TLegacyOwningKey LastKey_;

    std::vector<int> LastReadRowSessionIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessSortedMergingReader::TSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int sortKeyColumnCount,
    int reduceKeyColumnCount,
    bool interruptAtKeyEdge)
    : TSchemalessSortedMergingReaderBase(
        sortKeyColumnCount,
        reduceKeyColumnCount,
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

IUnversionedRowBatchPtr TSchemalessSortedMergingReader::Read(const TRowBatchReadOptions& options)
{
    LastReadRowSessionIndexes_.clear();

    if (!EnsureOpen(options) || !ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    auto* session = SessionHeap_.front();
    if (session->CurrentRowIndex == session->Rows.size()) {
        session->CurrentRowIndex = 0;
        if (ReadSession(session, options)) {
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

    TableRowIndex_ = session->Reader->GetTableRowIndex() - session->Rows.size() + session->CurrentRowIndex;

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;
    bool interrupting = Interrupting_;
    while (rows.size() < options.MaxRowsPerRead &&
           dataWeight < options.MaxDataWeightPerRead)
    {
        auto row = session->Rows[session->CurrentRowIndex];
        if (interrupting && CompareRows(row, LastKey_, ReduceKeyColumnCount_) != 0) {
            YT_LOG_DEBUG("Sorted merging reader interrupted (LastKey: %v, NextKey: %v)",
                LastKey_,
                GetKeyPrefix(row, ReduceKeyColumnCount_));
            SetReadyEvent(VoidFuture);
            SessionHeap_.clear();
            return rows.empty()
                ? nullptr
                : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
        }

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
            TableRowIndex_ = session->Reader->GetTableRowIndex() - session->Rows.size() + session->CurrentRowIndex;
        }
    }

    if (!rows.empty() && !interrupting) {
        LastKey_ = GetKeyPrefix(rows.back(), ReduceKeyColumnCount_);
    }

    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TInterruptDescriptor TSchemalessSortedMergingReader::GetInterruptDescriptor(
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

class TSchemalessJoiningReader
    : public TSchemalessSortedMergingReaderBase
{
public:
    TSchemalessJoiningReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
        int primaryKeyColumnCount,
        int reduceKeyColumnCount,
        const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
        int foreignKeyColumnCount,
        bool interruptAtKeyEdge);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

private:
    const bool InterruptAtKeyEdge_;

    TSession* PrimarySession_;
    TLegacyOwningKey LastPrimaryKey_;
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessJoiningReader::TSchemalessJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    int reduceKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount,
    bool interruptAtKeyEdge)
    : TSchemalessSortedMergingReaderBase(
        foreignKeyColumnCount,
        reduceKeyColumnCount,
        interruptAtKeyEdge)
    , InterruptAtKeyEdge_(interruptAtKeyEdge)
{
    YT_VERIFY(!primaryReaders.empty() && !foreignReaders.empty());
    YT_VERIFY(SortKeyColumnCount_ <= ReduceKeyColumnCount_);

    auto mergingReader = CreateSchemalessSortedMergingReader(
        primaryReaders,
        primaryKeyColumnCount,
        reduceKeyColumnCount,
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

IUnversionedRowBatchPtr TSchemalessJoiningReader::Read(const TRowBatchReadOptions& options)
{
    if (!EnsureOpen(options) || !ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
        return CreateEmptyUnversionedRowBatch();
    }

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    bool interrupting = Interrupting_;
    if (interrupting && SessionHeap_.front() == PrimarySession_ && !InterruptAtKeyEdge_) {
        // Extract primary session from session heap.
        ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
        SessionHeap_.pop_back();
    }

    if (SessionHeap_.empty()) {
        return nullptr;
    }

    auto* session = SessionHeap_.front();
    if (session->CurrentRowIndex == session->Rows.size()) {
        session->CurrentRowIndex = 0;
        if (ReadSession(session, options)) {
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

    TableRowIndex_ = session->Reader->GetTableRowIndex() - session->Rows.size() + session->CurrentRowIndex;

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;
    auto lastPrimaryRow = TLegacyKey(LastPrimaryKey_);
    auto nextPrimaryRow = TLegacyKey();
    while (rows.size() < options.MaxRowsPerRead &&
           dataWeight < options.MaxDataWeightPerRead)
    {
        auto row = session->Rows[session->CurrentRowIndex];
        if (interrupting) {
            if (CompareRows(
                row,
                lastPrimaryRow,
                (session == PrimarySession_) ? ReduceKeyColumnCount_ : SortKeyColumnCount_) > 0)
            {
                // Extract current session from session heap.
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

        bool shouldJoinRow = false;
        // Skip rows that are not present in PrimarySession_.
        if (session == PrimarySession_) {
            lastPrimaryRow = row;
            nextPrimaryRow = TLegacyKey();
            shouldJoinRow = true;
        } else {
            if (!nextPrimaryRow && PrimarySession_->CurrentRowIndex < PrimarySession_->Rows.size()) {
                nextPrimaryRow = PrimarySession_->Rows[PrimarySession_->CurrentRowIndex];
            }
            if (
                (CompareRows(row, lastPrimaryRow, SortKeyColumnCount_) == 0) ||
                (CompareRows(row, nextPrimaryRow, SortKeyColumnCount_) == 0))
            {
                shouldJoinRow = true;
            }
        }

        if (shouldJoinRow) {
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
            ++RowIndex_;
        } else {
            --RowCount_;
        }

        ++session->CurrentRowIndex;
        ++TableRowIndex_;

        if (session->CurrentRowIndex == session->Rows.size()) {
            // Out of prefetched rows in this session.
            break;
        }

        AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);

        if (SessionHeap_.front() != session) {
            session = SessionHeap_.front();
            TableRowIndex_ = session->Reader->GetTableRowIndex() - session->Rows.size() + session->CurrentRowIndex;
        }
    }

    if (lastPrimaryRow) {
        LastPrimaryKey_ = GetKeyPrefix(lastPrimaryRow, ReduceKeyColumnCount_);
    }

    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TInterruptDescriptor TSchemalessJoiningReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    YT_VERIFY(unreadRows.Empty());

    // Returns only UnreadDataSlices from primary readers.
    return PrimarySession_->Reader->GetInterruptDescriptor(
        PrimarySession_->Rows.Slice(
            PrimarySession_->CurrentRowIndex,
            PrimarySession_->Rows.Size()));
}

const TDataSliceDescriptor& TSchemalessJoiningReader::GetCurrentReaderDescriptor() const
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int sortKeyColumnCount,
    int reduceKeyColumnCount,
    bool interruptAtKeyEdge)
{
    YT_VERIFY(!readers.empty());
    // The only input reader can not satisfy key guarantee.
    if (readers.size() == 1 && !interruptAtKeyEdge) {
        return readers[0];
    } else {
        return New<TSchemalessSortedMergingReader>(
            readers,
            sortKeyColumnCount,
            reduceKeyColumnCount,
            interruptAtKeyEdge);
    }
}

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedJoiningReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& primaryReaders,
    int primaryKeyColumnCount,
    int reduceKeyColumnCount,
    const std::vector<ISchemalessMultiChunkReaderPtr>& foreignReaders,
    int foreignKeyColumnCount,
    bool interruptAtKeyEdge)
{
    YT_VERIFY(!primaryReaders.empty());
    YT_VERIFY(primaryKeyColumnCount >= reduceKeyColumnCount && reduceKeyColumnCount >= foreignKeyColumnCount);
    if (foreignReaders.empty()) {
        return CreateSchemalessSortedMergingReader(
            primaryReaders,
            primaryKeyColumnCount,
            reduceKeyColumnCount,
            interruptAtKeyEdge);
    } else {
        return New<TSchemalessJoiningReader>(
            primaryReaders,
            primaryKeyColumnCount,
            reduceKeyColumnCount,
            foreignReaders,
            foreignKeyColumnCount,
            interruptAtKeyEdge);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
