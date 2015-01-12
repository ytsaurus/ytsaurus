#include "stdafx.h"

#include "schemaless_sorted_merging_reader.h"

#include "private.h"
#include "schemaless_chunk_reader.h"

#include <ytlib/chunk_client/data_statistics.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <core/misc/heap.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

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
        bool enableTableIndex);

    virtual TFuture<void> Open() override;

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual int GetTableIndex() const override;

    virtual TDataStatistics GetDataStatistics() const override;

    virtual bool IsFetchingCompleted() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual i64 GetSessionRowCount() const override;

    virtual i64 GetSessionRowIndex() const override;

private:
    struct TSession
    {
        ISchemalessMultiChunkReaderPtr Reader;
        std::vector<TUnversionedRow> Rows;
        int CurrentRowIndex;
    };

    NLog::TLogger Logger;

    bool EnableTableIndex_;

    std::vector<TSession> SessionHolder_;
    std::vector<TSession*> SessionHeap_;

    i64 RowCount_ = 0;
    i64 RowIndex_ = 0;

    TFuture<void> ReadyEvent_;
    int TableIndex_;

    void DoOpen();

    friend bool CompareSessions(const TSession* lhs, const TSession* rhs);

};

////////////////////////////////////////////////////////////////////////////////

bool CompareSessions(
    const TSchemalessSortedMergingReader::TSession* lhs,
    const TSchemalessSortedMergingReader::TSession* rhs)
{
    int result = CompareRows(lhs->Rows[lhs->CurrentRowIndex], rhs->Rows[rhs->CurrentRowIndex]);
    if (result == 0) {
        result = lhs->Reader->GetTableIndex() - rhs->Reader->GetTableIndex();
    }
    return result < 0;
}

////////////////////////////////////////////////////////////////////////////////

TSchemalessSortedMergingReader::TSchemalessSortedMergingReader(
        const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
        bool enableTableIndex)
    : Logger(TableClientLogger)
    , EnableTableIndex_(enableTableIndex)
    , TableIndex_(0)
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

        RowCount_ += reader->GetSessionRowCount();
    }
}

TFuture<void> TSchemalessSortedMergingReader::Open()
{
    LOG_INFO(
        "Opening schemaless sorted merging reader (SessionCount: %d)",
        static_cast<int>(SessionHolder_.size()));

    ReadyEvent_ =  BIND(&TSchemalessSortedMergingReader::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();

    return ReadyEvent_;
}

void TSchemalessSortedMergingReader::DoOpen()
{
    std::vector<TFuture<void>> openErrors;
    for (auto& session : SessionHolder_) {
        openErrors.push_back(session.Reader->Open());
    }

    for (auto& asyncError : openErrors) {
        auto error = WaitFor(asyncError);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to open schemaless merging reader");
    }

    for (auto& session : SessionHolder_) {
        if (session.Reader->Read(&session.Rows)) {
            YCHECK(!session.Rows.empty());
            SessionHeap_.push_back(&session);
        }
    }

    if (!SessionHeap_.empty()) {
        MakeHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions);
    }
}

bool TSchemalessSortedMergingReader::Read(std::vector<TUnversionedRow> *rows)
{
    rows->clear();

    if (SessionHeap_.empty()) {
        return false;
    }

    auto* session = SessionHeap_.front();
    if (session->CurrentRowIndex == session->Rows.size()) {
        session->CurrentRowIndex = 0;
        if (!session->Reader->Read(&session->Rows)) {
            YCHECK(session->Rows.empty());
            ExtractHeap(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions);
            SessionHeap_.pop_back();
        } else if (session->Rows.empty()) {
            ReadyEvent_ = session->Reader->GetReadyEvent();
        } else {
            AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions);
        }

        return true;
    }

    TableIndex_ = session->Reader->GetTableIndex();
    while (rows->size() < rows->capacity()) {
        rows->push_back(session->Rows[session->CurrentRowIndex]);
        ++session->CurrentRowIndex;
        ++RowIndex_;

        if (session->CurrentRowIndex == session->Rows.size()) {
            // Out of prefetched rows in this session.
            return true;
        }

        AdjustHeapFront(SessionHeap_.begin(), SessionHeap_.end(), CompareSessions);

        if (EnableTableIndex_ && SessionHeap_.front() != session) {
            // Minimal reader changed, table index possibly changed as well.
            return true;
        }

        session = SessionHeap_.front();
    }

    return true;
}

TFuture<void> TSchemalessSortedMergingReader::GetReadyEvent()
{
    return ReadyEvent_;
}

int TSchemalessSortedMergingReader::GetTableIndex() const
{
    return TableIndex_;
}

TDataStatistics TSchemalessSortedMergingReader::GetDataStatistics() const
{
    auto dataStatistics = ZeroDataStatistics();

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

i64 TSchemalessSortedMergingReader::GetSessionRowCount() const
{
    return RowCount_;
}

i64 TSchemalessSortedMergingReader::GetSessionRowIndex() const
{
    return RowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortedMergingReader(
    const std::vector<ISchemalessMultiChunkReaderPtr>& readers,
    bool enableTableIndex)
{
    return New<TSchemalessSortedMergingReader>(readers, enableTableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
