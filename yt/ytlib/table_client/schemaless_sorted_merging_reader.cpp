#include "schemaless_sorted_merging_reader.h"
#include "private.h"
#include "schemaless_chunk_reader.h"
#include "name_table.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/misc/heap.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Reasonable default for max data size per one read call.
const i64 MaxDataSizePerRead = 16 * 1024 * 1024;
const i64 RowBufferSize = 10000;

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): unittests.
class TSchemalessSortedMergingReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        int keyColumnCount);

     virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TDataStatistics GetDataStatistics() const override;

    virtual bool IsFetchingCompleted() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual TKeyColumns GetKeyColumns() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual i64 GetSessionRowIndex() const override;

    virtual i64 GetTableRowIndex() const override;

private:
    struct TSession
    {
        ISchemalessMultiChunkReaderPtr Reader;
        std::vector<TUnversionedRow> Rows;
        int CurrentRowIndex;
        int TableIndex;
    };

    NLogging::TLogger Logger;

    int KeyColumnCount_;

    std::vector<TSession> SessionHolder_;
    std::vector<TSession*> SessionHeap_;

    i64 RowCount_ = 0;
    i64 RowIndex_ = 0;

    TFuture<void> ReadyEvent_;
    int TableIndex_ = 0;
    i64 TableRowIndex_ = 0;

    TOwningKey LastKey_;

    std::function<bool(const TSession* lhs, const TSession* rhs)> CompareSessions_;

    void DoOpen();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessSortedMergingReader::TSchemalessSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        int keyColumnCount)
    : Logger(TableClientLogger)
    , KeyColumnCount_(keyColumnCount)
{
    YCHECK(!readers.empty());
    int rowsPerSession = RowBufferSize / readers.size();

    YCHECK(rowsPerSession > 0);

    SessionHolder_.reserve(readers.size());
    SessionHeap_.reserve(readers.size());

    for (const auto& reader : readers) {
        SessionHolder_.push_back(TSession());
        auto& session = SessionHolder_.back();
        session.Reader = reader;
        session.Rows.reserve(rowsPerSession);
        session.CurrentRowIndex = 0;
        session.TableIndex = 0;

        RowCount_ += reader->GetTotalRowCount();
    }

    CompareSessions_ = [=] (const TSession* lhs, const TSession* rhs) -> bool {
        int result = CompareRows(
            lhs->Rows[lhs->CurrentRowIndex],
            rhs->Rows[rhs->CurrentRowIndex],
            KeyColumnCount_);
        if (result == 0) {
            result = lhs->TableIndex - rhs->TableIndex;
        }
        return result < 0;
    };

    LOG_INFO("Opening schemaless sorted merging reader (SessionCount: %v)",
        SessionHolder_.size());

    ReadyEvent_ =  BIND(&TSchemalessSortedMergingReader::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

void TSchemalessSortedMergingReader::DoOpen()
{
    auto getTableIndex = [] (TUnversionedRow row, TNameTablePtr nameTable) {
        auto tableIndexId = nameTable->GetIdOrRegisterName(TableIndexColumnName);
        for (auto valueIt = row.Begin(); valueIt != row.End(); ++valueIt) {
            if (valueIt->Id == tableIndexId) {
                YCHECK(valueIt->Type == EValueType::Int64);
                return valueIt->Data.Int64;
            }
        }
        return i64(0);
    };

    try {
        for (auto& session : SessionHolder_) {
            while (session.Reader->Read(&session.Rows)) {
                if (!session.Rows.empty()) {
                    session.TableIndex = getTableIndex(session.Rows.front(), session.Reader->GetNameTable());
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

bool TSchemalessSortedMergingReader::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);
    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return true;
    }

    rows->clear();

    if (SessionHeap_.empty()) {
        return false;
    }

    auto* session = SessionHeap_.front();
    if (session->CurrentRowIndex == session->Rows.size()) {
        session->CurrentRowIndex = 0;
        if (!session->Reader->Read(&session->Rows)) {
            YCHECK(session->Rows.empty());
            ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
            SessionHeap_.pop_back();
        } else if (session->Rows.empty()) {
            ReadyEvent_ = session->Reader->GetReadyEvent();
        } else {
            AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions_);
        }

        return true;
    }

    TableRowIndex_ = session->Reader->GetTableRowIndex() - session->Rows.size() + session->CurrentRowIndex;

    i64 dataWeight = 0;
    while (rows->size() < rows->capacity() && dataWeight < MaxDataSizePerRead) {
        rows->push_back(session->Rows[session->CurrentRowIndex]);
        dataWeight += GetDataWeight(rows->back());
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
    return true;
}

TFuture<void> TSchemalessSortedMergingReader::GetReadyEvent()
{
    return ReadyEvent_;
}

TDataStatistics TSchemalessSortedMergingReader::GetDataStatistics() const
{
    TDataStatistics dataStatistics;

    for (const auto& session : SessionHolder_) {
        dataStatistics += session.Reader->GetDataStatistics();
    }

    return dataStatistics;
}

bool TSchemalessSortedMergingReader::IsFetchingCompleted() const
{
    return std::all_of(
        SessionHolder_.begin(),
        SessionHolder_.end(),
        [] (const TSession& session) {
            return session.Reader->IsFetchingCompleted();
        });
}

std::vector<TChunkId> TSchemalessSortedMergingReader::GetFailedChunkIds() const
{
    std::vector<TChunkId> result;
    for (auto& session : SessionHolder_) {
        auto failedChunks = session.Reader->GetFailedChunkIds();
        result.insert(result.end(), failedChunks.begin(), failedChunks.end());
    }
    return result;
}

TNameTablePtr TSchemalessSortedMergingReader::GetNameTable() const
{
    return SessionHolder_.front().Reader->GetNameTable();
}

TKeyColumns TSchemalessSortedMergingReader::GetKeyColumns() const
{
    return SessionHolder_.front().Reader->GetKeyColumns();
}

i64 TSchemalessSortedMergingReader::GetTotalRowCount() const
{
    return RowCount_;
}

i64 TSchemalessSortedMergingReader::GetSessionRowIndex() const
{
    return RowIndex_;
}

i64 TSchemalessSortedMergingReader::GetTableRowIndex() const
{
    return TableRowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    int keyColumnCount)
{
    return New<TSchemalessSortedMergingReader>(readers, keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
