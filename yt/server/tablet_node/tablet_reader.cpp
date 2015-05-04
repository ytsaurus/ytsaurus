#include "stdafx.h"
#include "tablet_reader.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "partition.h"
#include "store.h"
#include "row_merger.h"
#include "config.h"
#include "private.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/small_vector.h>
#include <core/misc/heap.h>

#include <core/concurrency/scheduler.h>

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

static const auto& Logger = TabletNodeLogger;
static const size_t MaxRowsPerRead = 1024;
static const int TypicalStoresPerSession = 64;

////////////////////////////////////////////////////////////////////////////////

struct TTabletReaderPoolTag { };

// NB(lukyan): Do not use delegating constructors. It leads to incorrect generated code in Visual C++ and memory corruption.

#define DEFINE_MIXIN() \
    TParent* Parent() \
    { \
        return static_cast<TParent*>(this); \
    } \
     \
    const TParent* Parent() const \
    { \
        return static_cast<const TParent*>(this); \
    }

struct TSession
{
    IVersionedReaderPtr Reader;
    std::vector<TVersionedRow> Rows;
    std::vector<TVersionedRow>::iterator CurrentRow;
};

template <class TParent>
class TTabletReaderBase
{
public:
    TTabletReaderBase(
        IInvokerPtr poolInvoker,
        TTabletSnapshotPtr tabletSnapshot)
        : PoolInvoker_(std::move(poolInvoker))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , PerformanceCounters_(TabletSnapshot_->PerformanceCounters)
        , KeyComparer_(TabletSnapshot_->RowKeyComparer)
    { }

    const TDynamicRowKeyComparer& GetKeyComparer()
    {
        return KeyComparer_;
    }

    void AddExhausted(TSession* session)
    {
        ExhaustedSessions_.push_back(session);
    }

protected:
    const IInvokerPtr PoolInvoker_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TTabletPerformanceCountersPtr PerformanceCounters_;

    const TDynamicRowKeyComparer KeyComparer_;

    SmallVector<TSession, TypicalStoresPerSession> Sessions_;

    SmallVector<TSession*, TypicalStoresPerSession> ExhaustedSessions_;
    SmallVector<TSession*, TypicalStoresPerSession> RefillingSessions_;

    TFuture<void> ReadyEvent_ = VoidFuture;

    std::atomic<bool> Opened_ = {false};
    std::atomic<bool> Refilling_ = {false};

    template <class TRow>
    bool DoRead(std::vector<TRow>* rows)
    {
        YCHECK(Opened_);
        YCHECK(!Refilling_);

        rows->clear();
        Parent()->GetRowMerger()->Reset();

        if (!ExhaustedSessions_.empty()) {
            // Prevent proceeding to the merge phase in presence of exhausted sessions.
            // Request refill and signal the user that he must wait.
            if (RefillExhaustedSessions()) {
                return true;
            }
        }

        // Refill sessions with newly arrived rows requested in RefillExhaustedSessions above.
        for (auto* session : RefillingSessions_) {
            RefillSession(session);
        }
        RefillingSessions_.clear();

        // Check for the end-of-rowset.
        if (Parent()->NoActiveSessions()) {
            return false;
        }

        // Fetch equal by key rows

        // Must stop once an exhausted session appears.
        while (ExhaustedSessions_.empty()) {
            // Fetch rows from all sessions with a matching key and merge them.
            Parent()->FetchMatchingRows();

            // Save merged row.
            auto mergedRow = Parent()->GetRowMerger()->BuildMergedRow();
            if (mergedRow) {
                rows->push_back(mergedRow);
            }
        }

        PerformanceCounters_->MergedRowReadCount += rows->size();

        return true;
    }

    void AddReader(IVersionedReaderPtr reader)
    {
        if (reader) {
            Sessions_.push_back(TSession());
            auto& session = Sessions_.back();
            session.Reader = std::move(reader);
            session.Rows.reserve(MaxRowsPerRead);
        }
    }

    void DoOpen()
    {
        // Open readers.
        std::vector<TFuture<void>> asyncResults;
        for (const auto& session : Sessions_) {
            auto asyncResult = session.Reader->Open();
            auto maybeResult = asyncResult.TryGet();
            if (maybeResult) {
                maybeResult->ThrowOnError();
            } else {
                asyncResults.push_back(asyncResult);
            }
        }

        WaitFor(Combine(asyncResults))
            .ThrowOnError();

        // Mark all sessions as exhausted.
        for (auto& session : Sessions_) {
            ExhaustedSessions_.push_back(&session);
        }

        Opened_ = true;
    }

    bool RefillSession(TSession* session)
    {
        auto& rows = session->Rows;
        bool hasMoreRows = session->Reader->Read(&rows);

        if (rows.empty()) {
            return !hasMoreRows;
        }

        int rowCount = rows.size();
        PerformanceCounters_->UnmergedRowReadCount += rowCount;

        session->CurrentRow = rows.begin();
        
        Parent()->AddSessionToActive(session);
        return true;
    }

    bool RefillExhaustedSessions()
    {
        YCHECK(RefillingSessions_.empty());

        std::vector<TFuture<void>> asyncResults;
        for (auto* session : ExhaustedSessions_) {
            // Try to refill the session right away.
            if (!RefillSession(session)) {
                // No data at the moment, must wait.
                asyncResults.push_back(session->Reader->GetReadyEvent());
                RefillingSessions_.push_back(session);
            }
        }
        ExhaustedSessions_.clear();

        if (asyncResults.empty()) {
            return false;
        }

        Refilling_ = true;
        ReadyEvent_ = Combine(asyncResults).Apply(BIND([=, this_ = MakeStrong(Parent())] (const TError& error) {
            Refilling_ = false;
            error.ThrowOnError();
        }));

        return true;
    }

    TColumnFilter GetColumnFilter(const TTableSchema& schema)
    {
        const auto& tabletSchema = TabletSnapshot_->Schema;

        // Infer column filter.
        TColumnFilter columnFilter;
        columnFilter.All = false;
        for (const auto& column : schema.Columns()) {
            const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name);
            if (tabletColumn.Type != column.Type) {
                THROW_ERROR_EXCEPTION("Invalid type of schema column %Qv: expected %Qlv, actual %Qlv",
                    column.Name,
                    tabletColumn.Type,
                    column.Type);
            }
            columnFilter.Indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
        }

        return columnFilter;
    }

private:
    DEFINE_MIXIN()
};

template <class TParent>
class THeapMerger
{
public:
    void InitMerger(int maxSessionsCount)
    {
        // Construct an empty heap.
        ActiveSessions_.reserve(maxSessionsCount);
        ActiveSessionsBegin_ = ActiveSessionsEnd_ = ActiveSessions_.begin();
    }

    bool NoActiveSessions()
    {
        return ActiveSessionsBegin_ == ActiveSessionsEnd_;
    }

    std::function<bool(const TSession*, const TSession*)> GetSessionComparer()
    {
        return [&] (const TSession* lhsSession, const TSession* rhsSession) {
            auto lhsRow = *lhsSession->CurrentRow;
            auto rhsRow = *rhsSession->CurrentRow;
            return Parent()->GetKeyComparer()(
                lhsRow.BeginKeys(), lhsRow.EndKeys(),
                rhsRow.BeginKeys(), rhsRow.EndKeys()) < 0;
        };
    }

    const std::function<bool(const TSession*, const TSession*)> SessionComparer_ = GetSessionComparer();
    void AddSessionToActive(TSession* session)
    {
        *ActiveSessionsEnd_++ = session;
        AdjustHeapBack(ActiveSessionsBegin_, ActiveSessionsEnd_, SessionComparer_);
    }

    void FetchMatchingRows()
    {
        const TUnversionedValue* currentKeyBegin = nullptr;
        const TUnversionedValue* currentKeyEnd = nullptr;
        // Advance current rows in sessions.
        // Check for exhausted sessions.
        while (ActiveSessionsBegin_ != ActiveSessionsEnd_) {
            auto* session = *ActiveSessionsBegin_;
            auto partialRow = *session->CurrentRow;

            if (currentKeyBegin) {
                if (Parent()->GetKeyComparer()(
                        partialRow.BeginKeys(), partialRow.EndKeys(),
                        currentKeyBegin, currentKeyEnd) != 0)
                    break;
            } else {
                currentKeyBegin = partialRow.BeginKeys();
                currentKeyEnd = partialRow.EndKeys();
            }

            Parent()->GetRowMerger()->AddPartialRow(partialRow);

            if (++session->CurrentRow == session->Rows.end()) {
                Parent()->AddExhausted(session);
                ExtractHeap(ActiveSessionsBegin_, ActiveSessionsEnd_,  SessionComparer_);
                --ActiveSessionsEnd_;
            } else {
                #ifndef NDEBUG
                YASSERT(Parent()->GetKeyComparer()(
                    partialRow.BeginKeys(), partialRow.EndKeys(),
                    session->CurrentRow->BeginKeys(), session->CurrentRow->EndKeys()) < 0);
                #endif
                AdjustHeapFront(ActiveSessionsBegin_, ActiveSessionsEnd_,  SessionComparer_);
            }
        }
    }

private:
    typedef SmallVector<TSession*, TypicalStoresPerSession> TActiveSessions;
    TActiveSessions ActiveSessions_;
    typename TActiveSessions::iterator ActiveSessionsBegin_;
    typename TActiveSessions::iterator ActiveSessionsEnd_;

    DEFINE_MIXIN()

};

template <class TParent>
class TSimpleMerger
{
public:
    void InitMerger(int maxSessionsCount)
    {
        // Construct an empty heap.
        ActiveSessions_.reserve(maxSessionsCount);
        ActiveSessionsBegin_ = ActiveSessionsEnd_ = ActiveSessions_.begin();
    }

    bool NoActiveSessions()
    {
        return ActiveSessionsBegin_ == ActiveSessionsEnd_;
    }

    void AddSessionToActive(TSession* session)
    {
        *ActiveSessionsEnd_++ = session;
    }

    void FetchMatchingRows()
    {
        auto it = ActiveSessionsBegin_;
        while (it != ActiveSessionsEnd_) {
            auto* session = *it;
            auto partialRow = *session->CurrentRow;
            Parent()->GetRowMerger()->AddPartialRow(partialRow);

            if (++session->CurrentRow == session->Rows.end()) {
                Parent()->AddExhausted(session);
                --ActiveSessionsEnd_;
                std::swap(*it, *ActiveSessionsEnd_);
            } else {
                ++it;
            }
        }
    }

private:
    typedef SmallVector<TSession*, TypicalStoresPerSession> TActiveSessions;
    TActiveSessions ActiveSessions_;
    typename TActiveSessions::iterator ActiveSessionsBegin_;
    typename TActiveSessions::iterator ActiveSessionsEnd_;

    DEFINE_MIXIN()

};

////////////////////////////////////////////////////////////////////////////////

class TTabletRangeReader
    : public ISchemafulReader
    , public TTabletReaderBase<TTabletRangeReader>
    , public THeapMerger<TTabletRangeReader>
{
public:
    TTabletRangeReader(
        IInvokerPtr poolInvoker,
        TTabletSnapshotPtr tabletSnapshot,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp timestamp)
        : TTabletReaderBase(
            std::move(poolInvoker),
            std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , LowerBound_(std::move(lowerBound))
        , UpperBound_(std::move(upperBound))
        , Pool_(TTabletReaderPoolTag())
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        return BIND(&TTabletRangeReader::DoOpen, MakeStrong(this))
            .AsyncVia(PoolInvoker_)
            .Run(schema);
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return TBase::DoRead(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    TSchemafulRowMerger* GetRowMerger()
    {
        return RowMerger_.get();
    }

private:
    typedef TTabletReaderBase<TTabletRangeReader> TBase;

    TTimestamp Timestamp_;
    const TOwningKey LowerBound_;
    const TOwningKey UpperBound_;
    TChunkedMemoryPool Pool_;
    std::unique_ptr<TSchemafulRowMerger> RowMerger_;

    void DoOpen(const TTableSchema& schema)
    {
        // Select stores.
        std::vector<IStorePtr> stores;
        auto takePartition = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
            stores.insert(
                stores.end(),
                partitionSnapshot->Stores.begin(),
                partitionSnapshot->Stores.end());
        };

        takePartition(TabletSnapshot_->Eden);

        auto range = TabletSnapshot_->GetIntersectingPartitions(LowerBound_, UpperBound_);
        for (auto it = range.first; it != range.second; ++it) {
            takePartition(*it);
        }

        LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, StoreIds: [%v])",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->Slot->GetCellId(),
            Timestamp_,
            JoinToString(stores, TStoreIdFormatter()));

        if (stores.size() > TabletSnapshot_->Config->MaxReadFanIn) {
            THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
                << TErrorAttribute("tablet_id", TabletSnapshot_->TabletId)
                << TErrorAttribute("fan_in", stores.size())
                << TErrorAttribute("fan_in_limit", TabletSnapshot_->Config->MaxReadFanIn);
        }

        auto columnFilter = GetColumnFilter(schema);

        // Create readers.
        for (const auto& store : stores) {
            this->AddReader(store->CreateReader(
                LowerBound_,
                UpperBound_,
                Timestamp_,
                columnFilter));
        }

        RowMerger_ = std::make_unique<TSchemafulRowMerger>(&Pool_,
            TabletSnapshot_->Schema.Columns().size(),
            TabletSnapshot_->KeyColumns.size(),
            columnFilter);

        this->InitMerger(this->Sessions_.size());

        TBase::DoOpen();
    }

};

class TTabletKeysReader
    : public ISchemafulReader
    , public TTabletReaderBase<TTabletKeysReader>
    , public TSimpleMerger<TTabletKeysReader>
{
public:
    TTabletKeysReader(
        IInvokerPtr poolInvoker,
        TTabletSnapshotPtr tabletSnapshot,
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp)
        : TBase(
            std::move(poolInvoker),
            std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , Keys_(keys)
        , Pool_(TTabletReaderPoolTag())
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        return BIND(&TTabletKeysReader::DoOpen, MakeStrong(this))
            .AsyncVia(PoolInvoker_)
            .Run(schema);
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return TBase::DoRead(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    TSchemafulRowMerger* GetRowMerger()
    {
        return RowMerger_.get();
    }

private:
    typedef TTabletReaderBase<TTabletKeysReader> TBase;

    TTimestamp Timestamp_;
    const TSharedRange<TKey> Keys_;
    TChunkedMemoryPool Pool_;
    std::unique_ptr<TSchemafulRowMerger> RowMerger_;

    void DoOpen(const TTableSchema& schema)
    {
        // Select stores.
        std::vector<IStorePtr> stores;
        auto takePartition = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
            stores.insert(
                stores.end(),
                partitionSnapshot->Stores.begin(),
                partitionSnapshot->Stores.end());
        };

        takePartition(TabletSnapshot_->Eden);

        std::vector<TPartitionSnapshotPtr> snapshots;

        for (auto key : Keys_) {
            snapshots.push_back(TabletSnapshot_->FindContainingPartition(key));
        }

        std::sort(snapshots.begin(), snapshots.end());
        snapshots.erase(std::unique(snapshots.begin(), snapshots.end()), snapshots.end());

        for (const auto& snapshot : snapshots) {
            takePartition(snapshot);
        }

        LOG_DEBUG("Creating schemaful tablet reader (TabletId: %v, CellId: %v, Timestamp: %v, StoreIds: [%v])",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->Slot->GetCellId(),
            Timestamp_,
            JoinToString(stores, TStoreIdFormatter()));

        if (stores.size() > TabletSnapshot_->Config->MaxReadFanIn) {
            THROW_ERROR_EXCEPTION("Read fan-in limit exceeded; please wait until your data is merged")
                << TErrorAttribute("tablet_id", TabletSnapshot_->TabletId)
                << TErrorAttribute("fan_in", stores.size())
                << TErrorAttribute("fan_in_limit", TabletSnapshot_->Config->MaxReadFanIn);
        }

        auto columnFilter = GetColumnFilter(schema);

        // Create readers.
        for (const auto& store : stores) {
            this->AddReader(store->CreateReader(
                Keys_,
                Timestamp_,
                columnFilter));
        }

        RowMerger_ = std::make_unique<TSchemafulRowMerger>(&Pool_,
            TabletSnapshot_->Schema.Columns().size(),
            TabletSnapshot_->KeyColumns.size(),
            columnFilter);

        this->InitMerger(this->Sessions_.size());

        TBase::DoOpen();
    }

};


ISchemafulReaderPtr CreateSchemafulTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp)
{
    return New<TTabletRangeReader>(
        std::move(poolInvoker),
        std::move(tabletSnapshot),
        std::move(lowerBound),
        std::move(upperBound),
        timestamp);
}


ISchemafulReaderPtr CreateSchemafulTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp)
{
    return New<TTabletKeysReader>(
        std::move(poolInvoker),
        std::move(tabletSnapshot),
        std::move(keys),
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedTabletReader
    : public IVersionedReader
    , public TTabletReaderBase<TVersionedTabletReader>
    , public THeapMerger<TVersionedTabletReader>
{
public:
    TVersionedTabletReader(
        IInvokerPtr poolInvoker,
        TTabletSnapshotPtr tabletSnapshot,
        std::vector<IStorePtr> stores,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp)
        : TTabletReaderBase(
            std::move(poolInvoker),
            std::move(tabletSnapshot))
        , Stores_(std::move(stores))
        , RowMerger_(
            &Pool_,
            TabletSnapshot_->KeyColumns.size(),
            TabletSnapshot_->Config,
            currentTimestamp,
            majorTimestamp)
        , Timestamp_(AllCommittedTimestamp)
        , LowerBound_(std::move(lowerBound))
        , UpperBound_(std::move(upperBound))
    { }

    virtual TFuture<void> Open() override
    {
        return BIND(&TVersionedTabletReader::DoOpen, MakeStrong(this))
            .AsyncVia(PoolInvoker_)
            .Run();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        bool result = TTabletReaderBase::DoRead(rows);
        #ifndef NDEBUG
        for (int index = 0; index < static_cast<int>(rows->size()) - 1; ++index) {
            auto lhs = (*rows)[index];
            auto rhs = (*rows)[index + 1];
            YASSERT(KeyComparer_(
                lhs.BeginKeys(), lhs.EndKeys(),
                rhs.BeginKeys(), rhs.EndKeys()) < 0);
        }
        #endif
        return result;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    TVersionedRowMerger* GetRowMerger()
    {
        return &RowMerger_;
    }

private:
    typedef TTabletReaderBase<TVersionedTabletReader> TBase;

    const std::vector<IStorePtr> Stores_;
    TChunkedMemoryPool Pool_;
    TVersionedRowMerger RowMerger_;
    TTimestamp Timestamp_;
    const TOwningKey LowerBound_;
    const TOwningKey UpperBound_;
    

    void DoOpen()
    {
        LOG_DEBUG("Creating versioned tablet reader (TabletId: %v, CellId: %v, LowerBound: {%v}, UpperBound: {%v}, Timestamp: %v, StoreIds: [%v])",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->Slot->GetCellId(),
            LowerBound_,
            UpperBound_,
            Timestamp_,
            JoinToString(Stores_, TStoreIdFormatter()));

        for (const auto& store : Stores_) {
            this->AddReader(store->CreateReader(
                LowerBound_,
                UpperBound_,
                Timestamp_,
                TColumnFilter()));
        }

        TBase::DoOpen();
    }

};

IVersionedReaderPtr CreateVersionedTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<IStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp)
{
    return New<TVersionedTabletReader>(
        std::move(poolInvoker),
        std::move(tabletSnapshot),
        std::move(stores),
        std::move(lowerBound),
        std::move(upperBound),
        currentTimestamp,
        majorTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

