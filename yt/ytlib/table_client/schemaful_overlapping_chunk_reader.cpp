#include "schemaful_overlapping_chunk_reader.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"
#include "versioned_row.h"
#include "row_merger.h"
#include "versioned_reader.h"

#include <core/misc/heap.h>

#include <tuple>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 1024;
const int MinSimultaneousReaders = 5;

////////////////////////////////////////////////////////////////////////////////

class TSchemafulOverlappingChunkLookupReader
    : public ISchemafulReader
{
public:
    static ISchemafulReaderPtr Create(
        TSchemafulRowMergerPtr rowMerger,
        std::function<IVersionedReaderPtr()> readerFactory);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    struct TSession;
    TSchemafulRowMergerPtr RowMerger_;
    TFuture<void> ReadyEvent_;
    std::vector<TSession> Sessions_;
    std::vector<TSession*> AwaitingSessions_;
    bool Exhausted_ = false;

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

    TSchemafulOverlappingChunkLookupReader(TSchemafulRowMergerPtr rowMerger);
    bool RefillSession(TSession* sessions);
    void RefillSessions();
    void UpdateReadyEvent();

    DECLARE_NEW_FRIEND();
};

DECLARE_REFCOUNTED_CLASS(TSchemafulOverlappingChunkLookupReader)
DEFINE_REFCOUNTED_TYPE(TSchemafulOverlappingChunkLookupReader)

////////////////////////////////////////////////////////////////////////////////

TSchemafulOverlappingChunkLookupReader::TSchemafulOverlappingChunkLookupReader(
    TSchemafulRowMergerPtr rowMerger)
    : RowMerger_(std::move(rowMerger))
{ }

ISchemafulReaderPtr TSchemafulOverlappingChunkLookupReader::Create(
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory)
{
    auto this_ = New<TSchemafulOverlappingChunkLookupReader>(std::move(rowMerger));

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

TFuture<void> TSchemafulOverlappingChunkLookupReader::GetReadyEvent()
{
    return ReadyEvent_;
}

bool TSchemafulOverlappingChunkLookupReader::Read(std::vector<TUnversionedRow>* rows)
{
    auto readRow = [&] () {
        for (auto& session : Sessions_) {
            YASSERT(session.CurrentRow >= session.Rows.begin() && session.CurrentRow < session.Rows.end());
            RowMerger_->AddPartialRow(*session.CurrentRow);

            if (++session.CurrentRow == session.Rows.end()) {
                AwaitingSessions_.push_back(&session);
            }
        }

        auto row = RowMerger_->BuildMergedRow();
        rows->push_back(row);
    };

    rows->clear();
    RowMerger_->Reset();

    RefillSessions();

    if (!AwaitingSessions_.empty()) {
        return true;
    }

    while (AwaitingSessions_.empty() && !Exhausted_ && rows->size() < rows->capacity()) {
        readRow();
    }

    return !rows->empty() || !AwaitingSessions_.empty();
}

bool TSchemafulOverlappingChunkLookupReader::RefillSession(TSession* session)
{
    if (session->ReadyEvent && !session->ReadyEvent.IsSet()) {
        return false;
    }

    bool finished = !session->Reader->Read(&session->Rows);

    if (session->Rows.size() > 0) {
        session->CurrentRow = session->Rows.begin();
    } else if (finished) {
        Exhausted_ = true;
    }

    return finished || !session->Rows.empty();
}

void TSchemafulOverlappingChunkLookupReader::RefillSessions()
{
    if (AwaitingSessions_.size() == 0) {
        return;
    }

    std::vector<TSession*> awaitingSessions;

    for (auto* session : AwaitingSessions_) {
        if (!RefillSession(session)) {
            awaitingSessions.push_back(session);
            session->ReadyEvent = session->Reader->GetReadyEvent();
        }
    }

    AwaitingSessions_ = std::move(awaitingSessions);
    UpdateReadyEvent();
}

void TSchemafulOverlappingChunkLookupReader::UpdateReadyEvent()
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

ISchemafulReaderPtr CreateSchemafulOverlappingChunkLookupReader(
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory)
{
    return TSchemafulOverlappingChunkLookupReader::Create(
        std::move(rowMerger),
        std::move(readerFactory));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRowMerger>
class TSchemafulOverlappingChunkReaderBase
{
public:
    TSchemafulOverlappingChunkReaderBase(
        const std::vector<TOwningKey>& boundaries,
        TIntrusivePtr<TRowMerger> rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        const TOverlappingReaderKeyComparer& keyComparer,
        int minConcurrency);

    TFuture<void> DoOpen();

    template <class T>
    bool DoRead(std::vector<T>* rows);

    TFuture<void> DoGetReadyEvent();

private:
    struct TSession;
    class TSessionComparer;

    std::function<IVersionedReaderPtr(int index)> ReaderFactory_;
    TIntrusivePtr<TRowMerger> RowMerger_;
    TOverlappingReaderKeyComparer KeyComparer_;
    TFuture<void> ReadyEvent_;
    std::vector<TSession> Sessions_;
    std::vector<TSession*> ActiveSessions_;
    std::vector<TSession*> AwaitingSessions_;
    std::vector<TUnversionedValue> CurrentKey_;
    TSessionComparer SessionComparer_;
    int MinConcurrency_;
    int NextSession_ = 0;

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
            YCHECK(lhs->CurrentRow >= lhs->Rows.begin() && lhs->CurrentRow < lhs->Rows.end());
            YCHECK(rhs->CurrentRow >= rhs->Rows.begin() && rhs->CurrentRow < rhs->Rows.end());
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
    bool RefillSession(TSession* session);
    void RefillSessions();
    void UpdateReadyEvent();
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowMerger>
TSchemafulOverlappingChunkReaderBase<TRowMerger>::TSchemafulOverlappingChunkReaderBase(
    const std::vector<TOwningKey>& boundaries,
    TIntrusivePtr<TRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minConcurrency)
    : ReaderFactory_(std::move(readerFactory))
    , RowMerger_(std::move(rowMerger))
    , KeyComparer_(keyComparer)
    , SessionComparer_(KeyComparer_)
    , MinConcurrency_(minConcurrency)
{
    Sessions_.reserve(boundaries.size());
    for (int index = 0; index < boundaries.size(); ++index) {
        Sessions_.emplace_back(boundaries[index], index);
    }
    std::sort(Sessions_.begin(), Sessions_.end(), [] (const TSession& lhs, const TSession& rhs) {
        return std::make_tuple(lhs.Key, lhs.Index) < std::make_tuple(rhs.Key, rhs.Index);
    });
}

template <class TRowMerger>
TFuture<void> TSchemafulOverlappingChunkReaderBase<TRowMerger>::DoOpen()
{
    while (NextSession_ < Sessions_.size() && NextSession_ < MinConcurrency_) {
        OpenSession(NextSession_);
        ++NextSession_;
    }

    UpdateReadyEvent();
    return ReadyEvent_;
}

template <class TRowMerger>
template<class T>
bool TSchemafulOverlappingChunkReaderBase<TRowMerger>::DoRead(std::vector<T>* rows)
{
    auto readRow = [&] () {
        YASSERT(AwaitingSessions_.size() == 0);

        CurrentKey_.clear();

        while (ActiveSessions_.begin() != ActiveSessions_.end()) {
            auto* session = *ActiveSessions_.begin();
            auto partialRow = *session->CurrentRow;
            YASSERT(session->CurrentRow >= session->Rows.begin() && session->CurrentRow < session->Rows.end());

            if (!CurrentKey_.empty()) {
                if (KeyComparer_(
                        partialRow.BeginKeys(),
                        partialRow.EndKeys(),
                        &*CurrentKey_.begin(),
                        &*CurrentKey_.end()) != 0)
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
                #ifndef NDEBUG
                YASSERT(KeyComparer_(
                    partialRow.BeginKeys(), partialRow.EndKeys(),
                    session->CurrentRow->BeginKeys(), session->CurrentRow->EndKeys()) < 0);
                #endif
                AdjustHeapFront(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
            }
        }

        auto row = RowMerger_->BuildMergedRow();
        if (row) {
            rows->push_back(row);
        }
    };

    rows->clear();
    RowMerger_->Reset();

    RefillSessions();

    if (!AwaitingSessions_.empty()) {
        return true;
    }

    while (AwaitingSessions_.empty() && !ActiveSessions_.empty() && rows->size() < rows->capacity()) {
        readRow();
    }

    bool finished = ActiveSessions_.empty() && AwaitingSessions_.empty() && rows->empty();

    if (finished) {
        for (const auto& session : Sessions_) {
            YASSERT(!session.Reader);
        }
    }

    return !finished;
}

template <class TRowMerger>
TFuture<void> TSchemafulOverlappingChunkReaderBase<TRowMerger>::DoGetReadyEvent()
{
    return ReadyEvent_;
}

template <class TRowMerger>
void TSchemafulOverlappingChunkReaderBase<TRowMerger>::OpenSession(int index)
{
    Sessions_[index].Rows.reserve(MaxRowsPerRead);
    Sessions_[index].Reader = ReaderFactory_(Sessions_[index].Index);
    Sessions_[index].ReadyEvent = Sessions_[index].Reader->Open();
    AwaitingSessions_.push_back(&Sessions_[index]);
}

template <class TRowMerger>
bool TSchemafulOverlappingChunkReaderBase<TRowMerger>::RefillSession(TSession* session)
{
    if (session->ReadyEvent && !session->ReadyEvent.IsSet()) {
        return false;
    }

    bool finished = !session->Reader->Read(&session->Rows);

    if (session->Rows.size() > 0) {
        session->CurrentRow = session->Rows.begin();
        ActiveSessions_.push_back(session);
        AdjustHeapBack(ActiveSessions_.begin(), ActiveSessions_.end(), SessionComparer_);
    }

    if (finished && session->Rows.empty()) {
        session->Reader.Reset();
    }

    return finished || !session->Rows.empty();
}

template <class TRowMerger>
void TSchemafulOverlappingChunkReaderBase<TRowMerger>::RefillSessions()
{
    if (AwaitingSessions_.size() == 0) {
        return;
    }

    std::vector<TSession*> awaitingSessions;

    for (auto* session : AwaitingSessions_) {
        if (!RefillSession(session)) {
            awaitingSessions.push_back(session);
            session->ReadyEvent = session->Reader->GetReadyEvent();
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
void TSchemafulOverlappingChunkReaderBase<TRowMerger>::UpdateReadyEvent()
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

class TSchemafulOverlappingChunkReader
    : public ISchemafulReader
    , TSchemafulOverlappingChunkReaderBase<TSchemafulRowMerger>
{
public:
    static ISchemafulReaderPtr Create(
        const std::vector<TOwningKey>& boundaries,
        TSchemafulRowMergerPtr rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        const TOverlappingReaderKeyComparer& keyComparer,
        int minConcurrency)
    {
        auto this_ = New<TSchemafulOverlappingChunkReader>(
            boundaries,
            rowMerger,
            std::move(readerFactory),
            keyComparer,
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

private:
    TSchemafulOverlappingChunkReader(
        const std::vector<TOwningKey>& boundaries,
        TSchemafulRowMergerPtr rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        const TOverlappingReaderKeyComparer& keyComparer,
        int minConcurrency)
        : TSchemafulOverlappingChunkReaderBase<TSchemafulRowMerger>(
            boundaries,
            rowMerger,
            std::move(readerFactory),
            keyComparer,
            minConcurrency)
    { }

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulOverlappingChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TSchemafulRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders)
{
    return TSchemafulOverlappingChunkReader::Create(
        boundaries,
        rowMerger,
        std::move(readerFactory),
        keyComparer,
        minSimultaneousReaders);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedOverlappingChunkReader
    : public IVersionedReader
    , TSchemafulOverlappingChunkReaderBase<TVersionedRowMerger>
{
public:
    TVersionedOverlappingChunkReader(
        const std::vector<TOwningKey>& boundaries,
        TVersionedRowMergerPtr rowMerger,
        std::function<IVersionedReaderPtr(int index)> readerFactory,
        const TOverlappingReaderKeyComparer& keyComparer,
        int minConcurrency)
        : TSchemafulOverlappingChunkReaderBase<TVersionedRowMerger>(
            boundaries,
            rowMerger,
            std::move(readerFactory),
            keyComparer,
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
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedOverlappingChunkReader(
    const std::vector<TOwningKey>& boundaries,
    TVersionedRowMergerPtr rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    const TOverlappingReaderKeyComparer& keyComparer,
    int minSimultaneousReaders)
{
    return New<TVersionedOverlappingChunkReader>(
        boundaries,
        rowMerger,
        std::move(readerFactory),
        keyComparer,
        minSimultaneousReaders);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

