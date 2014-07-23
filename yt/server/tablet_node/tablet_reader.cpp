#include "stdafx.h"
#include "tablet_reader.h"
#include "tablet.h"
#include "partition.h"
#include "store.h"
#include "row_merger.h"
#include "config.h"
#include "private.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/small_vector.h>
#include <core/misc/heap.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/versioned_reader.h>

#include <atomic>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NVersionedTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 1024;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

class TTabletReaderBase
    : public virtual TRefCounted
{
public:
    TTabletReaderBase(
        TTablet* tablet,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp timestamp)
        : Tablet_(tablet)
        , LowerBound_(std::move(lowerBound))
        , UpperBound_(std::move(upperBound))
        , Timestamp_(timestamp)
        , AutomatonInvoker_(Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read))
        , Pool_(TTabletReaderPoolTag())
        , ReadyEvent_(OKFuture)
        , Opened_(false)
        , Refilling_(false)
    { }

protected:
    TTablet* Tablet_;
    TOwningKey LowerBound_;
    TOwningKey UpperBound_;
    TTimestamp Timestamp_;

    IInvokerPtr AutomatonInvoker_;

    TChunkedMemoryPool Pool_;

    struct TSession
    {
        IVersionedReaderPtr Reader;
        std::vector<TVersionedRow> Rows;
        std::vector<TVersionedRow>::iterator CurrentRow;
    };

    SmallVector<TSession, TypicalStoreCount> Sessions_;

    typedef SmallVector<TSession*, TypicalStoreCount> TSessionHeap; 
    TSessionHeap SessionHeap_;
    TSessionHeap::iterator SessionHeapBegin_;
    TSessionHeap::iterator SessionHeapEnd_;

    SmallVector<TSession*, TypicalStoreCount> ExhaustedSessions_;
    SmallVector<TSession*, TypicalStoreCount> RefillingSessions_;

    TAsyncError ReadyEvent_;

    std::atomic<bool> Opened_;
    std::atomic<bool> Refilling_;


    template <class TRow, class TRowMerger>
    bool DoRead(std::vector<TRow>* rows, TRowMerger* rowMerger)
    {
        YCHECK(Opened_);
        YCHECK(!Refilling_);

        rows->clear();
        Pool_.Clear();

        if (!ExhaustedSessions_.empty()) {
            // Prevent proceeding to the merge phase in presence of exhausted sessions.
            // Request refill and signal the user that he must wait.
            RefillExhaustedSessions();
            YCHECK(ExhaustedSessions_.empty()); // must be cleared in RefillSessions
            return true;
        }

        // Refill sessions with newly arrived rows requested in RefillExhausedSessions above.
        for (auto* session : RefillingSessions_) {
            RefillSession(session);
        }
        RefillingSessions_.clear();

        // Check for the end-of-rowset.
        if (SessionHeapBegin_ == SessionHeapEnd_) {
            return false;
        }

        // Must stop once an exhausted session appears.
        while (ExhaustedSessions_.empty()) {
            const TUnversionedValue* currentKeyBegin = nullptr;
            const TUnversionedValue* currentKeyEnd = nullptr;

            // Fetch rows from all sessions with a matching key and merge them.
            // Advance current rows in sessions.
            // Check for exhausted sessions.
            while (SessionHeapBegin_ != SessionHeapEnd_) {
                auto* session = *SessionHeapBegin_;
                auto partialRow = *session->CurrentRow;

                if (currentKeyBegin) {
                    if (CompareRows(
                            partialRow.BeginKeys(),
                            partialRow.EndKeys(),
                            currentKeyBegin,
                            currentKeyEnd) != 0)
                        break;
                } else {
                    currentKeyBegin = partialRow.BeginKeys();
                    currentKeyEnd = partialRow.EndKeys();
                }

                rowMerger->AddPartialRow(partialRow);

                if (++session->CurrentRow == session->Rows.end()) {
                    ExhaustedSessions_.push_back(session);
                    ExtractHeap(SessionHeapBegin_, SessionHeapEnd_, CompareSessions);
                    --SessionHeapEnd_;
                } else {
                    AdjustHeapFront(SessionHeapBegin_, SessionHeapEnd_, CompareSessions);
                }
            }

            // Save merged row.
            auto mergedRow = rowMerger->BuildMergedRow();
            if (mergedRow) {
                rows->push_back(mergedRow);
            }
        }
        
        return true;
    }

    void DoOpen(
        const TColumnFilter& columnFilter,
        const std::vector<IStorePtr>& stores)
    {
        // Create readers.
        for (const auto& store : stores) {
            auto reader = store->CreateReader(
                LowerBound_,
                UpperBound_,
                Timestamp_,
                columnFilter);
            if (reader) {
                Sessions_.push_back(TSession());
                auto& session = Sessions_.back();
                session.Reader = std::move(reader);
                session.Rows.reserve(MaxRowsPerRead);
            }
        }

        // Open readers.
        TIntrusivePtr<TParallelCollector<void>> openCollector;
        for (const auto& session : Sessions_) {
            auto asyncResult = session.Reader->Open();
            if (asyncResult.IsSet()) {
                THROW_ERROR_EXCEPTION_IF_FAILED(asyncResult.Get());
            } else {
                if (!openCollector) {
                    openCollector = New<TParallelCollector<void>>();
                }
                openCollector->Collect(asyncResult);
            }
        }

        if (openCollector) {
            auto result = WaitFor(openCollector->Complete());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        // Construct an empty heap.
        SessionHeap_.reserve(Sessions_.size());
        SessionHeapBegin_ = SessionHeapEnd_ = SessionHeap_.begin();

        // Mark all sessions as exhausted.
        for (auto& session : Sessions_) {
            ExhaustedSessions_.push_back(&session);
        }

        Opened_ = true;
    }


    static bool CompareSessions(const TSession* lhsSession, const TSession* rhsSession)
    {
        auto lhsRow = *lhsSession->CurrentRow;
        auto rhsRow = *rhsSession->CurrentRow;
        return CompareRows(
            lhsRow.BeginKeys(),
            lhsRow.EndKeys(),
            rhsRow.BeginKeys(),
            rhsRow.EndKeys()) < 0;
    }


    bool RefillSession(TSession* session)
    {
        bool hasMoreRows = session->Reader->Read(&session->Rows);
        if (session->Rows.empty()) {
            return !hasMoreRows;
        }
        session->CurrentRow = session->Rows.begin();
        *SessionHeapEnd_++ = session;
        AdjustHeapBack(SessionHeapBegin_, SessionHeapEnd_, CompareSessions);
        return true;
    }

    void RefillExhaustedSessions()
    {
        YCHECK(RefillingSessions_.empty());
        
        TIntrusivePtr<TParallelCollector<void>> refillCollector;
        for (auto* session : ExhaustedSessions_) {
            // Try to refill the session right away.
            if (!RefillSession(session)) {
                // No data at the moment, must wait.
                if (!refillCollector) {
                    refillCollector = New<TParallelCollector<void>>();
                }
                refillCollector->Collect(session->Reader->GetReadyEvent());
                RefillingSessions_.push_back(session);
            }
        }
        ExhaustedSessions_.clear();

        if (!refillCollector) {
            ReadyEvent_ = OKFuture;
            return;
        }

        auto this_ = MakeStrong(this);
        Refilling_ = true;
        ReadyEvent_ = refillCollector->Complete()
            .Apply(BIND([this, this_] (TError error) -> TError {
                Refilling_ = false;
                return error;
            }));
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulTabletReader
    : public TTabletReaderBase
    , public ISchemafulReader
{
public:
    TSchemafulTabletReader(
        TTablet* tablet,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp timestamp)
        : TTabletReaderBase(
            tablet,
            std::move(lowerBound),
            std::move(upperBound),
            timestamp)
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return BIND(&TSchemafulTabletReader::DoOpen, MakeStrong(this), schema)
            .Guarded()
            .AsyncVia(AutomatonInvoker_)
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return TTabletReaderBase::DoRead(rows, RowMerger_.get());
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    std::unique_ptr<TUnversionedRowMerger> RowMerger_;


    void DoOpen(const TTableSchema& schema)
    {
        const auto& tabletSchema = Tablet_->Schema();

        // Infer column filter.
        TColumnFilter columnFilter;
        columnFilter.All = false;
        for (const auto& column : schema.Columns()) {
            const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name);
            if (tabletColumn.Type != column.Type) {
                THROW_ERROR_EXCEPTION("Invalid type of schema column %s: expected %s, actual %s",
                    ~column.Name.Quote(),
                    ~FormatEnum(tabletColumn.Type).Quote(),
                    ~FormatEnum(column.Type).Quote());
            }
            columnFilter.Indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
        }

        // Initialize merger.
        RowMerger_.reset(new TUnversionedRowMerger(
            &Pool_,
            Tablet_->GetSchemaColumnCount(),
            Tablet_->GetKeyColumnCount(),
            columnFilter));

        // Select stores.
        std::vector<IStorePtr> stores;
        auto takePartition = [&] (TPartition* partition) {
            for (auto store : partition->Stores()) {
                stores.push_back(std::move(store));
            }
        };

        takePartition(Tablet_->GetEden());

        auto range = Tablet_->GetIntersectingPartitions(LowerBound_, UpperBound_);
        for (auto it = range.first; it != range.second; ++it) {
            takePartition(it->get());
        }

        TTabletReaderBase::DoOpen(columnFilter, stores);
    }

};

ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTablet* tablet,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp)
{
    YCHECK(tablet);

    return New<TSchemafulTabletReader>(
        tablet,
        std::move(lowerBound),
        std::move(upperBound),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedTabletReader
    : public TTabletReaderBase
    , public IVersionedReader
{
public:
    TVersionedTabletReader(
        TTablet* tablet,
        std::vector<IStorePtr> stores,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp)
        : TTabletReaderBase(
            tablet,
            std::move(lowerBound),
            std::move(upperBound),
            AllCommittedTimestamp)
        , Stores_(std::move(stores))
        , CurrentTimestamp_(currentTimestamp)
        , MajorTimestamp_(majorTimestamp)
    { }

    virtual TAsyncError Open() override
    {
        return BIND(&TVersionedTabletReader::DoOpen, MakeStrong(this))
            .Guarded()
            .AsyncVia(AutomatonInvoker_)
            .Run();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        return TTabletReaderBase::DoRead(rows, RowMerger_.get());
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    std::vector<IStorePtr> Stores_;
    TTimestamp CurrentTimestamp_;
    TTimestamp MajorTimestamp_;

    std::unique_ptr<TVersionedRowMerger> RowMerger_;


    void DoOpen()
    {
        // Initialize merger.
        RowMerger_.reset(new TVersionedRowMerger(
            &Pool_,
            Tablet_->GetKeyColumnCount(),
            Tablet_->GetConfig(),
            CurrentTimestamp_,
            MajorTimestamp_));

        TTabletReaderBase::DoOpen(TColumnFilter(), Stores_);
    }

};

IVersionedReaderPtr CreateVersionedTabletReader(
    TTablet* tablet,
    std::vector<IStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp)
{
    return New<TVersionedTabletReader>(
        tablet,
        std::move(stores),
        std::move(lowerBound),
        std::move(upperBound),
        currentTimestamp,
        majorTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

