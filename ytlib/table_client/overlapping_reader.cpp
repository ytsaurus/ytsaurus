#include "overlapping_reader.h"
#include "row_merger.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"
#include "versioned_reader.h"
#include "versioned_row.h"

#include <yt/ytlib/chunk_client/data_statistics.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/heap.h>

#include <tuple>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

class TSchemafulOverlappingLookupReader
    : public ISchemafulReader
{
public:
    static ISchemafulReaderPtr Create(
        std::unique_ptr<TSchemafulRowMerger> rowMerger,
        std::function<IVersionedReaderPtr()> readerFactory);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TDataStatistics GetDataStatistics() const override;

private:
    struct TSession
    {
        IVersionedReaderPtr Reader;
        TFuture<void> ReadyEvent;
        std::vector<TVersionedRow> Rows;
        std::vector<TVersionedRow>::iterator CurrentRow;

        TSession(IVersionedReaderPtr reader)
            : Reader(std::move(reader))
        { }
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

    DECLARE_NEW_FRIEND();
};

DECLARE_REFCOUNTED_CLASS(TSchemafulOverlappingLookupReader)
DEFINE_REFCOUNTED_TYPE(TSchemafulOverlappingLookupReader)

////////////////////////////////////////////////////////////////////////////////

TSchemafulOverlappingLookupReader::TSchemafulOverlappingLookupReader(
    std::unique_ptr<TSchemafulRowMerger> rowMerger)
    : RowMerger_(std::move(rowMerger))
{ }

ISchemafulReaderPtr TSchemafulOverlappingLookupReader::Create(
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory)
{
    auto this_ = New<TSchemafulOverlappingLookupReader>(std::move(rowMerger));

    while (auto reader = readerFactory()) {
        this_->Sessions_.emplace_back(reader);
        auto& session = this_->Sessions_.back();
        session.Rows.reserve(MaxRowsPerRead);
        session.ReadyEvent = session.Reader->Open();
    }

    for (auto& session : this_->Sessions_) {
        this_->AwaitingSessions_.push_back(&session);
    }

    this_->UpdateReadyEvent();
    return this_;
}

TFuture<void> TSchemafulOverlappingLookupReader::GetReadyEvent()
{
    return ReadyEvent_;
}

TDataStatistics TSchemafulOverlappingLookupReader::GetDataStatistics() const
{
    TDataStatistics dataStatistics;
    for (const auto& session : Sessions_) {
        dataStatistics += session.Reader->GetDataStatistics();
    }
    dataStatistics.set_row_count(RowCount_);
    dataStatistics.set_data_weight(DataWeight_);
    return dataStatistics;
}

bool TSchemafulOverlappingLookupReader::Read(std::vector<TUnversionedRow>* rows)
{
    i64 dataWeight = 0;
    auto readRow = [&] () {
        for (auto& session : Sessions_) {
            Y_ASSERT(session.CurrentRow >= session.Rows.begin() && session.CurrentRow < session.Rows.end());
            RowMerger_->AddPartialRow(*session.CurrentRow);

            if (++session.CurrentRow == session.Rows.end()) {
                AwaitingSessions_.push_back(&session);
            }
        }

        auto row = RowMerger_->BuildMergedRow();
        rows->push_back(row);
        dataWeight += GetDataWeight(row);
    };

    rows->clear();
    RowMerger_->Reset();

    RefillSessions();

    while (AwaitingSessions_.empty() && !Exhausted_ && rows->size() < rows->capacity()) {
        readRow();
    }

    RowCount_ += rows->size();
    DataWeight_ += dataWeight;

    return !rows->empty() || !AwaitingSessions_.empty();
}

bool TSchemafulOverlappingLookupReader::RefillSession(TSession* session)
{
    YCHECK(session->ReadyEvent);

    if (!session->ReadyEvent.IsSet()) {
        return false;
    }

    bool finished = !session->Reader->Read(&session->Rows);

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
    ReadyEvent_ = Combine(readyEvents);
}

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulOverlappingLookupReader(
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
public:
    TSchemafulOverlappingRangeReaderBase(
        const std::vector<TOwningKey>& boundaries,
        std::unique_ptr<TRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        TOverlappingReaderKeyComparer keyComparer,
        int minConcurrency);

    TFuture<void> DoOpen();

    bool DoRead(std::vector<typename TRowMerger::TResultingRow>* rows);

    TFuture<void> DoGetReadyEvent();

    TDataStatistics DoGetDataStatistics() const;

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
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TReaderWriterSpinLock SpinLock_;

    struct TSession
    {
        TOwningKey Key;
        int Index;
        IVersionedReaderPtr Reader;
        TFuture<void> ReadyEvent;
        std::vector<TVersionedRow> Rows;
        std::vector<TVersionedRow>::iterator CurrentRow;

        TSession(TOwningKey key, int index)
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
            Y_ASSERT(lhs->CurrentRow >= lhs->Rows.begin() && lhs->CurrentRow < lhs->Rows.end());
            Y_ASSERT(rhs->CurrentRow >= rhs->Rows.begin() && rhs->CurrentRow < rhs->Rows.end());
            return KeyComparer_(
                lhs->CurrentRow->BeginKeys(),
                lhs->CurrentRow->EndKeys(),
                rhs->CurrentRow->BeginKeys(),
                rhs->CurrentRow->EndKeys()) <= 0;
        }

    private:
        const TOverlappingReaderKeyComparer& KeyComparer_;
    };

    void OpenSession(int index);
    bool RefillSession(TSession* session, size_t readerCapacity);
    void RefillSessions(size_t readerCapacity);
    void UpdateReadyEvent();
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowMerger>
TSchemafulOverlappingRangeReaderBase<TRowMerger>::TSchemafulOverlappingRangeReaderBase(
    const std::vector<TOwningKey>& boundaries,
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
    for (int index = 0; index < boundaries.size(); ++index) {
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
            TReaderGuard guard(SpinLock_);
            reader = session.Reader;
        }
        if (reader) {
            dataStatistics += reader->GetDataStatistics();
        }
    }

    dataStatistics.set_row_count(RowCount_);
    dataStatistics.set_data_weight(DataWeight_);
    return dataStatistics;
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoIsFetchingCompleted() const
{
    if (NextSession_ < Sessions_.size() || AwaitingSessions_.empty()) {
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
    while (NextSession_ < Sessions_.size() && NextSession_ < MinConcurrency_) {
        OpenSession(NextSession_);
        ++NextSession_;
    }

    UpdateReadyEvent();
    return ReadyEvent_;
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoRead(
    std::vector<typename TRowMerger::TResultingRow>* rows)
{
    auto readRow = [&] () {
        Y_ASSERT(AwaitingSessions_.empty());

        CurrentKey_.clear();

        while (ActiveSessions_.begin() != ActiveSessions_.end()) {
            auto* session = *ActiveSessions_.begin();
            auto partialRow = *session->CurrentRow;

            Y_ASSERT(session->CurrentRow >= session->Rows.begin() && session->CurrentRow < session->Rows.end());

            if (!CurrentKey_.empty()) {
                if (KeyComparer_(
                        partialRow.BeginKeys(),
                        partialRow.EndKeys(),
                        CurrentKey_.data(),
                        CurrentKey_.data() + CurrentKey_.size()) != 0)
                {
                    break;
                }
            } else {
                CurrentKey_.resize(partialRow.GetKeyCount());
                std::copy(partialRow.BeginKeys(), partialRow.EndKeys(), CurrentKey_.begin());

                int index = NextSession_;

                while (index < Sessions_.size() &&
                    KeyComparer_(
                        partialRow.BeginKeys(),
                        partialRow.EndKeys(),
                        Sessions_[index].Key.Begin(),
                        Sessions_[index].Key.End()) >= 0)
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
                Y_ASSERT(KeyComparer_(
                    partialRow.BeginKeys(), partialRow.EndKeys(),
                    session->CurrentRow->BeginKeys(), session->CurrentRow->EndKeys()) < 0);
                AdjustHeapFront(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
            }
        }

        auto row = RowMerger_->BuildMergedRow();
        if (row) {
            rows->push_back(row);
            ++RowCount_;
            DataWeight_ += GetDataWeight(row);
        }
    };

    rows->clear();
    RowMerger_->Reset();

    RefillSessions(std::min(rows->capacity(), MaxRowsPerRead));

    while (AwaitingSessions_.empty() && !ActiveSessions_.empty() && rows->size() < rows->capacity()) {
        readRow();
    }

    bool finished = ActiveSessions_.empty() && AwaitingSessions_.empty() && rows->empty();

    if (finished) {
        for (const auto& session : Sessions_) {
            Y_ASSERT(!session.Reader);
        }
    }

    return !finished;
}

template <class TRowMerger>
TFuture<void> TSchemafulOverlappingRangeReaderBase<TRowMerger>::DoGetReadyEvent()
{
    return ReadyEvent_;
}

template <class TRowMerger>
void TSchemafulOverlappingRangeReaderBase<TRowMerger>::OpenSession(int index)
{
    auto reader = ReaderFactory_(Sessions_[index].Index);
    {
        TWriterGuard guard(SpinLock_);
        Sessions_[index].Reader = std::move(reader);
    }
    Sessions_[index].ReadyEvent = Sessions_[index].Reader->Open();
    AwaitingSessions_.push_back(&Sessions_[index]);
}

template <class TRowMerger>
bool TSchemafulOverlappingRangeReaderBase<TRowMerger>::RefillSession(TSession* session, size_t readerCapacity)
{
    YCHECK(session->ReadyEvent);

    if (!session->ReadyEvent.IsSet()) {
        return false;
    }

    session->Rows.reserve(readerCapacity);
    bool finished = !session->Reader->Read(&session->Rows);

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
        {
            TWriterGuard guard(SpinLock_);
            DataStatistics_ += dataStatistics;
            session->Reader.Reset();
        }
    } else {
        session->ReadyEvent = session->Reader->GetReadyEvent();
    }

    return finished || !session->Rows.empty();
}

template <class TRowMerger>
void TSchemafulOverlappingRangeReaderBase<TRowMerger>::RefillSessions(size_t readerCapacity)
{
    if (AwaitingSessions_.empty()) {
        return;
    }

    std::vector<TSession*> awaitingSessions;

    for (auto* session : AwaitingSessions_) {
        if (!RefillSession(session, readerCapacity)) {
            awaitingSessions.push_back(session);
        }
    }

    AwaitingSessions_ = std::move(awaitingSessions);

    while (AwaitingSessions_.size() + ActiveSessions_.size() < MinConcurrency_ &&
        NextSession_ < Sessions_.size())
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
    ReadyEvent_ = Combine(readyEvents);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemafulOverlappingRangeReader
    : public ISchemafulReader
    , public TSchemafulOverlappingRangeReaderBase<TSchemafulRowMerger>
{
public:
    static ISchemafulReaderPtr Create(
        const std::vector<TOwningKey>& boundaries,
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

        this_->DoOpen();

        return this_;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return DoRead(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return DoGetReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return DoGetDataStatistics();
    }

private:
    TSchemafulOverlappingRangeReader(
        const std::vector<TOwningKey>& boundaries,
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

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulOverlappingRangeReader(
    const std::vector<TOwningKey>& boundaries,
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
    , TSchemafulOverlappingRangeReaderBase<TVersionedRowMerger>
{
public:
    TVersionedOverlappingRangeReader(
        const std::vector<TOwningKey>& boundaries,
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

    virtual TFuture<void> Open() override
    {
        return DoOpen();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        return DoRead(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return DoGetReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return DoGetDataStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return DoIsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return DoGetFailedChunkIds();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedOverlappingRangeReader(
    const std::vector<TOwningKey>& boundaries,
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

} // namespace NTableClient
} // namespace NYT

