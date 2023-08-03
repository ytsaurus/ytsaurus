#include "overlapping_reader.h"
#include "row_merger.h"

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/heap.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <tuple>

namespace NYT::NTableClient {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 MaxRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

class TSchemafulOverlappingLookupReader
    : public ISchemafulUnversionedReader
{
public:
    static ISchemafulUnversionedReaderPtr Create(
        std::unique_ptr<TSchemafulRowMerger> rowMerger,
        std::function<IVersionedReaderPtr()> readerFactory);

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;
    TFuture<void> GetReadyEvent() const override;

    TDataStatistics GetDataStatistics() const override;
    TCodecStatistics GetDecompressionStatistics() const override;

    bool IsFetchingCompleted() const override;
    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

private:
    struct TSession
    {
        IVersionedReaderPtr Reader;
        TFuture<void> ReadyEvent;
        IVersionedRowBatchPtr RowBatch;
        std::vector<TVersionedRow> Rows;
        std::vector<TVersionedRow>::iterator CurrentRow;

        explicit TSession(IVersionedReaderPtr reader)
            : Reader(std::move(reader))
        { }

        TSession(const TSession&) = delete;
        TSession(TSession&&) = default;
    };

    std::unique_ptr<TSchemafulRowMerger> RowMerger_;
    TFuture<void> ReadyEvent_;
    std::vector<TSession> Sessions_;
    std::vector<TSession*> AwaitingSessions_;
    bool Exhausted_ = false;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TSchemafulOverlappingLookupReader(std::unique_ptr<TSchemafulRowMerger> rowMerger);
    bool RefillSession(TSession* sessions);
    void RefillSessions();
    void UpdateReadyEvent();

    DECLARE_NEW_FRIEND()
};

DECLARE_REFCOUNTED_CLASS(TSchemafulOverlappingLookupReader)
DEFINE_REFCOUNTED_TYPE(TSchemafulOverlappingLookupReader)

////////////////////////////////////////////////////////////////////////////////

TSchemafulOverlappingLookupReader::TSchemafulOverlappingLookupReader(
    std::unique_ptr<TSchemafulRowMerger> rowMerger)
    : RowMerger_(std::move(rowMerger))
{ }

ISchemafulUnversionedReaderPtr TSchemafulOverlappingLookupReader::Create(
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory)
{
    auto this_ = New<TSchemafulOverlappingLookupReader>(std::move(rowMerger));

    while (auto reader = readerFactory()) {
        auto& session = this_->Sessions_.emplace_back(reader);
        session.Rows.reserve(MaxRowsPerRead);
        session.ReadyEvent = session.Reader->Open();
    }

    for (auto& session : this_->Sessions_) {
        this_->AwaitingSessions_.push_back(&session);
    }

    this_->UpdateReadyEvent();
    this_->Exhausted_ = this_->Sessions_.empty();
    return this_;
}

TFuture<void> TSchemafulOverlappingLookupReader::GetReadyEvent() const
{
    return ReadyEvent_;
}

TDataStatistics TSchemafulOverlappingLookupReader::GetDataStatistics() const
{
    TDataStatistics dataStatistics;
    for (const auto& session : Sessions_) {
        dataStatistics += session.Reader->GetDataStatistics();
    }

    dataStatistics.set_unmerged_row_count(dataStatistics.row_count());
    dataStatistics.set_unmerged_data_weight(dataStatistics.data_weight());

    dataStatistics.set_row_count(RowCount_);
    dataStatistics.set_data_weight(DataWeight_);
    return dataStatistics;
}

TCodecStatistics TSchemafulOverlappingLookupReader::GetDecompressionStatistics() const
{
    TCodecStatistics result;
    for (const auto& session : Sessions_) {
        result += session.Reader->GetDecompressionStatistics();
    }
    return result;
}

bool TSchemafulOverlappingLookupReader::IsFetchingCompleted() const
{
    for (const auto& session : Sessions_) {
        if (!session.Reader->IsFetchingCompleted()) {
            return false;
        }
    }
    return true;
}

std::vector<NChunkClient::TChunkId> TSchemafulOverlappingLookupReader::GetFailedChunkIds() const
{
    std::vector<NChunkClient::TChunkId> result;
    for (const auto& session : Sessions_) {
        auto failedChunkIds = session.Reader->GetFailedChunkIds();
        result.insert(result.end(), failedChunkIds.begin(), failedChunkIds.end());
    }
    return result;
}

IUnversionedRowBatchPtr TSchemafulOverlappingLookupReader::Read(const TRowBatchReadOptions& options)
{
    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    auto readRow = [&] () {
        for (auto& session : Sessions_) {
            YT_ASSERT(session.CurrentRow >= session.Rows.begin() && session.CurrentRow < session.Rows.end());
            RowMerger_->AddPartialRow(*session.CurrentRow);

            if (++session.CurrentRow == session.Rows.end()) {
                AwaitingSessions_.push_back(&session);
            }
        }

        auto row = RowMerger_->BuildMergedRow();
        rows.push_back(row);
        dataWeight += GetDataWeight(row);
    };

    RowMerger_->Reset();

    RefillSessions();

    while (AwaitingSessions_.empty() &&
           !Exhausted_ &&
           std::ssize(rows) < options.MaxRowsPerRead &&
           dataWeight < options.MaxDataWeightPerRead)
    {
        readRow();
    }

    RowCount_ += rows.size();
    DataWeight_ += dataWeight;

    if (rows.empty() && AwaitingSessions_.empty()) {
        return nullptr;
    }

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

bool TSchemafulOverlappingLookupReader::RefillSession(TSession* session)
{
    YT_VERIFY(session->ReadyEvent);

    if (!session->ReadyEvent.IsSet() || !session->ReadyEvent.Get().IsOK()) {
        return false;
    }

    TRowBatchReadOptions options{
        .MaxRowsPerRead = MaxRowsPerRead
    };
    session->RowBatch = session->Reader->Read(options);

    bool finished = !static_cast<bool>(session->RowBatch);
    if (!finished) {
        auto range = session->RowBatch->MaterializeRows();
        session->Rows = std::vector<TVersionedRow>(range.begin(), range.end());
    } else {
        session->Rows.clear();
    }

    if (!session->Rows.empty()) {
        session->CurrentRow = session->Rows.begin();
    } else if (finished) {
        Exhausted_ = true;
        session->ReadyEvent.Reset();
    } else {
        session->ReadyEvent = session->Reader->GetReadyEvent();
    }

    return finished || !session->Rows.empty();
}

void TSchemafulOverlappingLookupReader::RefillSessions()
{
    if (AwaitingSessions_.empty()) {
        return;
    }

    std::vector<TSession*> awaitingSessions;

    for (auto* session : AwaitingSessions_) {
        if (!RefillSession(session)) {
            awaitingSessions.push_back(session);
        }
    }

    AwaitingSessions_ = std::move(awaitingSessions);
    UpdateReadyEvent();
}

void TSchemafulOverlappingLookupReader::UpdateReadyEvent()
{
    std::vector<TFuture<void>> readyEvents;
    for (auto* session : AwaitingSessions_) {
        if (session->ReadyEvent) {
            readyEvents.push_back(session->ReadyEvent);
        }
    }
    ReadyEvent_ = AllSucceeded(readyEvents);
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulOverlappingLookupReader(
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory)
{
    return TSchemafulOverlappingLookupReader::Create(
        std::move(rowMerger),
        std::move(readerFactory));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRowMerger>
class TSchemafulOverlappingRangeReaderBase
{
protected:
    TSchemafulOverlappingRangeReaderBase(
        const std::vector<TLegacyOwningKey>& boundaries,
        std::unique_ptr<TRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        TOverlappingReaderKeyComparer keyComparer,
        int minConcurrency);

    TFuture<void> DoOpen();

    bool DoRead(
        std::vector<typename TRowMerger::TResultingRow>* rows,
        const TRowBatchReadOptions& options);

    TFuture<void> DoGetReadyEvent() const;

    TDataStatistics DoGetDataStatistics() const;

    TCodecStatistics DoGetDecompressionStatistics() const;

    bool DoIsFetchingCompleted() const;

    std::vector<TChunkId> DoGetFailedChunkIds() const;

private:
    struct TSession;
    class TSessionComparer;

    std::function<IVersionedReaderPtr(int index)> ReaderFactory_;
    std::unique_ptr<TRowMerger> RowMerger_;
    TOverlappingReaderKeyComparer KeyComparer_;
    TFuture<void> ReadyEvent_;
    std::vector<TSession> Sessions_;
    std::vector<TSession*> ActiveSessions_;
    std::vector<TSession*> AwaitingSessions_;
    std::vector<TUnversionedValue> CurrentKey_;
    TSessionComparer SessionComparer_;
    int MinConcurrency_;
    int NextSession_ = 0;

    TDataStatistics DataStatistics_;
    TCodecStatistics DecompressionStatistics_;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    struct TSession
    {
        TLegacyOwningKey Key;
        int Index;
        IVersionedReaderPtr Reader;
        TFuture<void> ReadyEvent;
        IVersionedRowBatchPtr RowBatch;
        std::vector<TVersionedRow> Rows;
        std::vector<TVersionedRow>::iterator CurrentRow;

        TSession(TLegacyOwningKey key, int index)
            : Key(std::move(key))
            , Index(index)
        { }
    };

    class TSessionComparer
    {
    public:
        TSessionComparer(const TOverlappingReaderKeyComparer& keyComparer)
            : KeyComparer_(keyComparer)
        { }

        bool operator()(const TSession* lhs, const TSession* rhs) const
        {
            YT_ASSERT(lhs->CurrentRow >= lhs->Rows.begin() && lhs->CurrentRow < lhs->Rows.end());
            YT_ASSERT(rhs->CurrentRow >= rhs->Rows.begin() && rhs->CurrentRow < rhs->Rows.end());
            return KeyComparer_(lhs->CurrentRow->Keys(), rhs->CurrentRow->Keys()) <= 0;
        }

    private:
        const TOverlappingReaderKeyComparer& KeyComparer_;
    };

    void OpenSession(int index);
    bool RefillSession(TSession* session, const TRowBatchReadOptions& options);
    void RefillSessions(const TRowBatchReadOptions& options);
    void UpdateReadyEvent();
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowMerger>
TSchemafulOverlappingRangeReaderBase<TRowMerger>::TSchemafulOverlappingRangeReaderBase(
    const std::vector<TLegacyOwningKey>& boundaries,
    std::unique_ptr<TRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency)
    : ReaderFactory_(std::move(readerFactory))
    , RowMerger_(std::move(rowMerger))
    , KeyComparer_(std::move(keyComparer))
    , SessionComparer_(KeyComparer_)
    , MinConcurrency_(minConcurrency)
{
    Sessions_.reserve(boundaries.size());
    for (int index = 0; index < std::ssize(boundaries); ++index) {
        Sessions_.emplace_back(boundaries[index], index);
    }
    std::sort(Sessions_.begin(), Sessions_.end(), [&] (const TSession& lhs, const TSession& rhs) {
        return std::tie(lhs.Key, lhs.Index) < std::tie(rhs.Key, rhs.Index);
    });
}

template <class TRowMerger>
TDataStatistics TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoGetDataStatistics() const
{
    auto dataStatistics = DataStatistics_;

    for (const auto& session : Sessions_) {
        IVersionedReaderPtr reader;
        {
            auto guard = ReaderGuard(SpinLock_);
            reader = session.Reader;
        }
        if (reader) {
            dataStatistics += reader->GetDataStatistics();
        }
    }

    dataStatistics.set_unmerged_row_count(dataStatistics.row_count());
    dataStatistics.set_unmerged_data_weight(dataStatistics.data_weight());

    dataStatistics.set_row_count(RowCount_);
    dataStatistics.set_data_weight(DataWeight_);
    return dataStatistics;
}

template <class TRowMerger>
TCodecStatistics TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoGetDecompressionStatistics() const
{
    std::vector<IVersionedReaderPtr> readers;
    TCodecStatistics result;
    {
        auto guard = ReaderGuard(SpinLock_);
        result = DecompressionStatistics_;
        readers.reserve(Sessions_.size());
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                readers.push_back(session.Reader);
            }
        }
    }

    for (const auto& reader : readers) {
        result += reader->GetDecompressionStatistics();
    }

    return result;
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoIsFetchingCompleted() const
{
    if (NextSession_ < std::ssize(Sessions_) || AwaitingSessions_.empty()) {
        return false;
    }

    for (const auto& session : ActiveSessions_) {
        if (!session->Reader->IsFetchingCompleted()) {
            return false;
        }
    }

    return true;
}

template <class TRowMerger>
std::vector<TChunkId> TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoGetFailedChunkIds() const
{
    THashSet<TChunkId> failedChunkIds;
    for (const auto& session : AwaitingSessions_) {
        auto sessionChunkIds = session->Reader->GetFailedChunkIds();
        failedChunkIds.insert(sessionChunkIds.begin(), sessionChunkIds.end());
    }

    for (const auto& session : ActiveSessions_) {
        auto sessionChunkIds = session->Reader->GetFailedChunkIds();
        failedChunkIds.insert(sessionChunkIds.begin(), sessionChunkIds.end());
    }

    return std::vector<TChunkId>(failedChunkIds.begin(), failedChunkIds.end());
}

template <class TRowMerger>
TFuture<void> TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoOpen()
{
    while (NextSession_ < std::ssize(Sessions_) && NextSession_ < MinConcurrency_) {
        OpenSession(NextSession_);
        ++NextSession_;
    }

    UpdateReadyEvent();
    return ReadyEvent_;
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoRead(
    std::vector<typename TRowMerger::TResultingRow>* rows,
    const TRowBatchReadOptions& options)
{
    rows->clear();
    RowMerger_->Reset();

    RefillSessions(options);

    i64 dataWeight = 0;
    auto readRow = [&] {
        YT_ASSERT(AwaitingSessions_.empty());

        CurrentKey_.clear();

        while (ActiveSessions_.begin() != ActiveSessions_.end()) {
            auto* session = *ActiveSessions_.begin();
            auto partialRow = *session->CurrentRow;

            YT_ASSERT(session->CurrentRow >= session->Rows.begin() && session->CurrentRow < session->Rows.end());

            if (!CurrentKey_.empty()) {
                if (KeyComparer_(partialRow.Keys(), MakeRange(CurrentKey_)) != 0) {
                    break;
                }
            } else {
                CurrentKey_.resize(partialRow.GetKeyCount());
                std::copy(partialRow.BeginKeys(), partialRow.EndKeys(), CurrentKey_.begin());

                int index = NextSession_;

                while (index < std::ssize(Sessions_) &&
                    KeyComparer_(partialRow.Keys(), Sessions_[index].Key.Elements()) >= 0)
                {
                    OpenSession(index);
                    ++index;
                }

                if (index > NextSession_) {
                    NextSession_ = index;
                    break;
                }
            }

            RowMerger_->AddPartialRow(partialRow);

            if (++session->CurrentRow == session->Rows.end()) {
                AwaitingSessions_.push_back(session);
                ExtractHeap(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
                ActiveSessions_.pop_back();
            } else {
                YT_ASSERT(KeyComparer_(partialRow.Keys(), session->CurrentRow->Keys()) < 0);
                AdjustHeapFront(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
            }
        }

        if (auto row = RowMerger_->BuildMergedRow()) {
            rows->push_back(row);
            dataWeight += GetDataWeight(row);
        }
    };

    while (
        AwaitingSessions_.empty() &&
        !ActiveSessions_.empty() &&
        std::ssize(*rows) < options.MaxRowsPerRead &&
        dataWeight < options.MaxDataWeightPerRead)
    {
        readRow();
    }

    RowCount_ += rows->size();
    DataWeight_ += dataWeight;

    bool finished = ActiveSessions_.empty() && AwaitingSessions_.empty() && rows->empty();

    if (finished) {
        for (const auto& session : Sessions_) {
            YT_ASSERT(!session.Reader);
        }
    }

    return !finished;
}

template <class TRowMerger>
TFuture<void> TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoGetReadyEvent() const
{
    return ReadyEvent_;
}

template <class TRowMerger>
void TSchemafulOverlappingRangeReaderBase<TRowMerger>::OpenSession(int index)
{
    auto reader = ReaderFactory_(Sessions_[index].Index);
    {
        auto guard = WriterGuard(SpinLock_);
        Sessions_[index].Reader = std::move(reader);
    }
    Sessions_[index].ReadyEvent = Sessions_[index].Reader->Open();
    AwaitingSessions_.push_back(&Sessions_[index]);
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::RefillSession(
    TSession* session,
    const TRowBatchReadOptions& options)
{
    YT_VERIFY(session->ReadyEvent);

    if (!session->ReadyEvent.IsSet() || !session->ReadyEvent.Get().IsOK()) {
        return false;
    }

    session->RowBatch = session->Reader->Read(options);

    bool finished = !static_cast<bool>(session->RowBatch);
    if (!finished) {
        auto range = session->RowBatch->MaterializeRows();
        session->Rows = std::vector<TVersionedRow>(range.begin(), range.end());
    } else {
        session->Rows.clear();
    }

    session->Rows.erase(
        std::remove_if(
            session->Rows.begin(),
            session->Rows.end(),
            [] (TVersionedRow row) {
                return !row;
            }),
        session->Rows.end());

    if (!session->Rows.empty()) {
        session->CurrentRow = session->Rows.begin();
        ActiveSessions_.push_back(session);
        AdjustHeapBack(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
    } else if (finished) {
        auto dataStatistics = session->Reader->GetDataStatistics();
        auto decompressionStatistics = session->Reader->GetDecompressionStatistics();
        {
            auto guard = WriterGuard(SpinLock_);
            DataStatistics_ += dataStatistics;
            DecompressionStatistics_ += decompressionStatistics;
            session->Reader.Reset();
        }
    } else {
        session->ReadyEvent = session->Reader->GetReadyEvent();
    }

    return finished || !session->Rows.empty();
}

template <class TRowMerger>
void TSchemafulOverlappingRangeReaderBase<TRowMerger>::RefillSessions(const TRowBatchReadOptions& options)
{
    if (AwaitingSessions_.empty()) {
        return;
    }

    std::vector<TSession*> awaitingSessions;

    for (auto* session : AwaitingSessions_) {
        if (!RefillSession(session, options)) {
            awaitingSessions.push_back(session);
        }
    }

    AwaitingSessions_ = std::move(awaitingSessions);

    while (std::ssize(AwaitingSessions_) + std::ssize(ActiveSessions_) < MinConcurrency_ &&
        NextSession_ < std::ssize(Sessions_))
    {
        OpenSession(NextSession_);
        ++NextSession_;
    }

    UpdateReadyEvent();
}

template <class TRowMerger>
void TSchemafulOverlappingRangeReaderBase<TRowMerger>::UpdateReadyEvent()
{
    std::vector<TFuture<void>> readyEvents;
    for (auto* session : AwaitingSessions_) {
        if (session->ReadyEvent) {
            readyEvents.push_back(session->ReadyEvent);
        }
    }
    ReadyEvent_ = AllSucceeded(readyEvents);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemafulOverlappingRangeReader
    : public ISchemafulUnversionedReader
    , public TSchemafulOverlappingRangeReaderBase<TSchemafulRowMerger>
{
public:
    static ISchemafulUnversionedReaderPtr Create(
        const std::vector<TLegacyOwningKey>& boundaries,
        std::unique_ptr<TSchemafulRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        TOverlappingReaderKeyComparer keyComparer,
        int minConcurrency)
    {
        auto this_ = New<TSchemafulOverlappingRangeReader>(
            boundaries,
            std::move(rowMerger),
            std::move(readerFactory),
            std::move(keyComparer),
            minConcurrency);

        YT_UNUSED_FUTURE(this_->DoOpen());

        return this_;
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        if (!DoRead(&rows, options)) {
            return nullptr;
        }
        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return DoGetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return DoGetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return DoGetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return DoIsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return DoGetFailedChunkIds();
    }

private:
    TSchemafulOverlappingRangeReader(
        const std::vector<TLegacyOwningKey>& boundaries,
        std::unique_ptr<TSchemafulRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        TOverlappingReaderKeyComparer keyComparer,
        int minConcurrency)
        : TSchemafulOverlappingRangeReaderBase<TSchemafulRowMerger>(
            boundaries,
            std::move(rowMerger),
            std::move(readerFactory),
            std::move(keyComparer),
            minConcurrency)
    { }

    DECLARE_NEW_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulOverlappingRangeReader(
    const std::vector<TLegacyOwningKey>& boundaries,
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency)
{
    return TSchemafulOverlappingRangeReader::Create(
        boundaries,
        std::move(rowMerger),
        std::move(readerFactory),
        std::move(keyComparer),
        minConcurrency);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedOverlappingRangeReader
    : public IVersionedReader
    , public TSchemafulOverlappingRangeReaderBase<TVersionedRowMerger>
{
public:
    TVersionedOverlappingRangeReader(
        const std::vector<TLegacyOwningKey>& boundaries,
        std::unique_ptr<TVersionedRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        TOverlappingReaderKeyComparer keyComparer,
        int minConcurrency)
        : TSchemafulOverlappingRangeReaderBase<TVersionedRowMerger>(
            boundaries,
            std::move(rowMerger),
            std::move(readerFactory),
            std::move(keyComparer),
            minConcurrency)
    { }

    TFuture<void> Open() override
    {
        return DoOpen();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        if (!DoRead(&rows, options)) {
            return nullptr;
        }
        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return DoGetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return DoGetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return DoGetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return DoIsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return DoGetFailedChunkIds();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedOverlappingRangeReader(
    const std::vector<TLegacyOwningKey>& boundaries,
    std::unique_ptr<TVersionedRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency)
{
    return New<TVersionedOverlappingRangeReader>(
        boundaries,
        std::move(rowMerger),
        std::move(readerFactory),
        std::move(keyComparer),
        minConcurrency);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

