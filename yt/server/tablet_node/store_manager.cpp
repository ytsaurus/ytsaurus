#include "stdafx.h"
#include "store_manager.h"
#include "tablet.h"
#include "dynamic_memory_store.h"
#include "config.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/chunked_memory_pool.h>
#include <core/misc/small_vector.h>
#include <core/misc/heap.h>

#include <core/concurrency/fiber.h>
#include <core/concurrency/parallel_collector.h>

#include <ytlib/object_client/public.h>

#include <ytlib/tablet_client/protocol.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/schemed_reader.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/api/transaction.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NObjectClient;
using namespace NApi;

using NVersionedTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;
static const size_t MaxRowsPerRead = 1024;
static const size_t TypicalStoreCount = 64;

////////////////////////////////////////////////////////////////////////////////

class TStoreManager::TRowMerger
{
public:
    TRowMerger(
        TChunkedMemoryPool* pool,
        int schemaColumnCount,
        int keyColumnCount,
        const TColumnFilter& columnFilter)
        : Pool_(pool)
        , SchemaColumnCount_(schemaColumnCount)
        , KeyColumnCount_(keyColumnCount)
        , KeyComparer_(KeyColumnCount_)
        , MergedValues_(SchemaColumnCount_)
        , ColumnFlags_(SchemaColumnCount_)
    {
        if (columnFilter.All) {
            for (int id = 0; id < schemaColumnCount; ++id) {
                ColumnFlags_[id] = true;
                ColumnIds_.push_back(id);
            }
        } else {
            for (int id : columnFilter.Indexes) {
                if (id < 0 || id >= schemaColumnCount) {
                    THROW_ERROR_EXCEPTION("Invalid column id %d in column filter",
                        id);
                }
                ColumnFlags_[id] = true;
                ColumnIds_.push_back(id);
            }
        }
    }

    void Start(const TUnversionedValue* keyBegin)
    {
        CurrentTimestamp_ = NullTimestamp | TombstoneTimestampMask;
        for (int id = 0; id < KeyColumnCount_; ++id) {
            MergedValues_[id] = MakeVersionedValue(keyBegin[id], NullTimestamp);
        }
    }

    void AddPartialRow(TVersionedRow row)
    {
        YASSERT(row.GetKeyCount() == KeyColumnCount_);
        YASSERT(row.GetTimestampCount() == 1);
        
        auto rowTimestamp = row.BeginTimestamps()[0];
        if ((rowTimestamp & TimestampValueMask) < (CurrentTimestamp_ & TimestampValueMask))
            return;
        
        if (rowTimestamp & TombstoneTimestampMask) {
            CurrentTimestamp_ = rowTimestamp;
        } else {
            if ((CurrentTimestamp_ & TombstoneTimestampMask) || !(rowTimestamp & IncrementalTimestampMask)) {
                CurrentTimestamp_ = rowTimestamp & TimestampValueMask;
                for (int id = KeyColumnCount_; id < SchemaColumnCount_; ++id) {
                    MergedValues_[id] = MakeVersionedSentinelValue(EValueType::Null, CurrentTimestamp_);
                }
            }

            const auto* rowValues = row.BeginValues();
            for (int index = 0; index < row.GetValueCount(); ++index) {
                const auto& value = rowValues[index];
                int id = value.Id;
                if (ColumnFlags_[id] && MergedValues_[id].Timestamp <= value.Timestamp) {
                    MergedValues_[id] = value;
                }
            }
        }
    }

    TUnversionedRow BuildMergedRow(bool skipNulls)
    {
        if (CurrentTimestamp_ & TombstoneTimestampMask) {
            return TUnversionedRow();
        }

        auto row = TUnversionedRow::Allocate(Pool_, ColumnIds_.size());
        auto* outputValue = row.Begin();
        for (int id : ColumnIds_) {
            const auto& mergedValue = MergedValues_[id];
            if (!skipNulls || mergedValue.Type != EValueType::Null) {
                *outputValue++ = mergedValue;
            }
        }

        if (skipNulls) {
            // Adjust row length.
            row.SetCount(outputValue - row.Begin());
        }

        return row;
    }

private:
    TChunkedMemoryPool* Pool_;
    int SchemaColumnCount_;
    int KeyColumnCount_;

    TKeyComparer KeyComparer_;
    SmallVector<TVersionedValue, TypicalColumnCount> MergedValues_;
    SmallVector<bool, TypicalColumnCount> ColumnFlags_;
    SmallVector<int, TypicalColumnCount> ColumnIds_;
    
    TTimestamp CurrentTimestamp_;

};

////////////////////////////////////////////////////////////////////////////////

static auto PresetResult = MakeFuture(TError());

class TStoreManager::TReader
    : public ISchemedReader
{
public:
    TReader(
        TStoreManagerPtr owner,
        TOwningKey lowerBound,
        TOwningKey upperBound,
        TTimestamp timestamp)
        : Owner_(std::move(owner))
        , LowerBound_(std::move(lowerBound))
        , UpperBound_(std::move(upperBound))
        , Timestamp_(timestamp)
        , ReadyEvent_(PresetResult)
    { }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        return BIND(&TReader::DoOpen, MakeStrong(this), schema)
            .AsyncVia(Owner_->Tablet_->GetEpochAutomatonInvoker())
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();

        // Check for end-of-stream.
        if (SessionHeapBegin_ == SessionHeapEnd_) {
            return false;
        }

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
                    RowMerger_->Start(currentKeyBegin);
                }

                RowMerger_->AddPartialRow(partialRow);

                if (++session->CurrentRow == session->Rows.end()) {
                    ExhaustedSessions_.push_back(session);
                    ExtractHeap(SessionHeapBegin_, SessionHeapEnd_, CompareSessions);
                    --SessionHeapEnd_;
                } else {
                    AdjustHeapFront(SessionHeapBegin_, SessionHeapEnd_, CompareSessions);
                }
            }

            // Save merged row.
            auto mergedRow = RowMerger_->BuildMergedRow(false);
            if (mergedRow) {
                rows->push_back(mergedRow);
            }
        }
        
        if (rows->empty()) {
            RefillSessions();
        }

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    TStoreManagerPtr Owner_;
    TOwningKey LowerBound_;
    TOwningKey UpperBound_;
    TTimestamp Timestamp_;

    TChunkedMemoryPool Pool_;
    std::unique_ptr<TRowMerger> RowMerger_;

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

    TError DoOpen(const TTableSchema& schema)
    {
        try {
            auto* tablet = Owner_->Tablet_;
            const auto& tabletSchema = tablet->Schema();

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
            RowMerger_.reset(new TRowMerger(
                &Pool_,
                tablet->GetSchemaColumnCount(),
                tablet->GetKeyColumnCount(),
                columnFilter));


            // Construct readers.
            for (const auto& pair : Owner_->Tablet_->Stores()) {
                const auto& store = pair.second;
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

            // Construct session heap.
            SessionHeap_.reserve(Sessions_.size());
            SessionHeapBegin_ = SessionHeapEnd_ = SessionHeap_.begin();
            for (auto& session : Sessions_) {
                TryRefillSession(&session);
            }

            return TError();
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    bool TryRefillSession(TSession* session)
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

    void RefillSessions()
    {
        YCHECK(RefillingSessions_.empty());
        
        TIntrusivePtr<TParallelCollector<void>> refillCollector;
        for (auto* session : ExhaustedSessions_) {
            // Try to refill the session right away.
            if (!TryRefillSession(session)) {
                // No data at the moment, must wait.
                if (!refillCollector) {
                    refillCollector = New<TParallelCollector<void>>();
                }
                refillCollector->Collect(session->Reader->GetReadyEvent());
                RefillingSessions_.push_back(session);
            }
        }

        if (refillCollector) {
            auto this_ = MakeStrong(this);
            ReadyEvent_ = refillCollector->Complete()
                .Apply(BIND([this, this_] (TError error) -> TError {
                    if (error.IsOK()) {
                        for (auto* session : RefillingSessions_) {
                            TryRefillSession(session);
                        }
                    }

                    RefillingSessions_.clear();
                    return error;
                }));
        } else {
            ReadyEvent_ = PresetResult;
        }
    }


};

////////////////////////////////////////////////////////////////////////////////

TStoreManager::TStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* Tablet_)
    : Config_(config)
    , Tablet_(Tablet_)
    , RotationScheduled_(false)
{
    YCHECK(Config_);
    YCHECK(Tablet_);

    VersionedPooledRows_.reserve(MaxRowsPerRead);
}

TStoreManager::~TStoreManager()
{ }

TTablet* TStoreManager::GetTablet() const
{
    return Tablet_;
}

bool TStoreManager::HasActiveLocks() const
{
    if (Tablet_->GetActiveStore()->GetLockCount() > 0) {
        return true;
    }
   
    if (!LockedStores_.empty()) {
        return true;
    }

    return false;
}

bool TStoreManager::HasUnflushedStores() const
{
    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        auto state = store->GetState();
        if (state != EStoreState::Persistent) {
            return true;
        }
    }
    return false;
}

void TStoreManager::LookupRow(
    TTimestamp timestamp,
    NTabletClient::TProtocolReader* reader,
    NTabletClient::TProtocolWriter* writer)
{
    auto key = TOwningKey(reader->ReadUnversionedRow());
    auto columnFilter = reader->ReadColumnFilter();

    int keyColumnCount = Tablet_->GetKeyColumnCount();
    int schemaColumnCount = Tablet_->GetSchemaColumnCount();

    SmallVector<bool, TypicalColumnCount> columnFilterFlags(schemaColumnCount);
    if (columnFilter.All) {
        for (int id = 0; id < schemaColumnCount; ++id) {
            columnFilterFlags[id] = true;
        }
    } else {
        for (int index : columnFilter.Indexes) {
            if (index < 0 || index >= schemaColumnCount) {
                THROW_ERROR_EXCEPTION("Invalid index %d in column filter",
                    index);
            }
            columnFilterFlags[index] = true;
        }
    }

    auto keySuccessor = GetKeySuccessor(key.Get());

    // Construct readers.
    SmallVector<IVersionedReaderPtr, TypicalStoreCount> rowReaders;
    for (const auto& pair : Tablet_->Stores()) {
        const auto& store = pair.second;
        auto rowReader = store->CreateReader(
            key,
            keySuccessor,
            timestamp,
            columnFilter);
        if (rowReader) {
            rowReaders.push_back(std::move(rowReader));
        }
    }

    // Open readers.
    TIntrusivePtr<TParallelCollector<void>> openCollector;
    for (const auto& reader : rowReaders) {
        auto asyncResult = reader->Open();
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

    TKeyComparer keyComparer(keyColumnCount);

    TRowMerger rowMerger(
        &LookupPool_,
        schemaColumnCount,
        keyColumnCount,
        columnFilter);

    rowMerger.Start(key.Begin());

    // Merge values.
    for (const auto& reader : rowReaders) {
        VersionedPooledRows_.clear();
        // NB: Reading at most one row.
        reader->Read(&VersionedPooledRows_);
        if (VersionedPooledRows_.empty())
            continue;

        auto partialRow = VersionedPooledRows_[0];
        if (keyComparer(key, partialRow.BeginKeys()) != 0)
            continue;

        rowMerger.AddPartialRow(partialRow);
    }

    auto mergedRow = rowMerger.BuildMergedRow(true);
    UnversionedPooledRow_.clear();
    if (mergedRow) {
        UnversionedPooledRow_.push_back(mergedRow);
    }
    writer->WriteUnversionedRowset(UnversionedPooledRow_);
}

ISchemedReaderPtr TStoreManager::CreateReader(
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp)
{
    return New<TReader>(
        this,
        std::move(lowerBound),
        std::move(upperBound),
        timestamp);
}

void TStoreManager::WriteRow(
    TTransaction* transaction,
    TUnversionedRow row,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    CheckLockAndMaybeMigrateRow(
        transaction,
        row,
        ERowLockMode::Write);

    auto dynamicRow = Tablet_->GetActiveStore()->WriteRow(
        transaction,
        row,
        prewrite);

    if (lockedRows && dynamicRow) {
        lockedRows->push_back(dynamicRow);
    }
}

void TStoreManager::DeleteRow(
    TTransaction* transaction,
    NVersionedTableClient::TKey key,
    bool prewrite,
    std::vector<TDynamicRow>* lockedRows)
{
    CheckLockAndMaybeMigrateRow(
        transaction,
        key,
        ERowLockMode::Delete);

    auto dynamicRow = Tablet_->GetActiveStore()->DeleteRow(
        transaction,
        key,
        prewrite);

    if (lockedRows && dynamicRow) {
        lockedRows->push_back(dynamicRow);
    }
}

void TStoreManager::ConfirmRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->ConfirmRow(row);
}

void TStoreManager::PrepareRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->PrepareRow(row);
}

void TStoreManager::CommitRow(const TDynamicRowRef& rowRef)
{
    auto row = MaybeMigrateRow(rowRef);
    Tablet_->GetActiveStore()->CommitRow(row);
}

void TStoreManager::AbortRow(const TDynamicRowRef& rowRef)
{
    // NB: Even passive store can handle this.
    rowRef.Store->AbortRow(rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

TDynamicRow TStoreManager::MaybeMigrateRow(const TDynamicRowRef& rowRef)
{
    if (rowRef.Store->GetState() == EStoreState::ActiveDynamic) {
        return rowRef.Row;
    }

    auto migrateFrom = rowRef.Store;
    const auto& migrateTo = Tablet_->GetActiveStore();
    auto migratedRow = migrateFrom->MigrateRow(rowRef.Row, migrateTo);

    CheckForUnlockedStore(migrateFrom);

    return migratedRow;
}

void TStoreManager::CheckLockAndMaybeMigrateRow(
    TTransaction* transaction,
    TUnversionedRow key,
    ERowLockMode mode)
{
    for (const auto& store : LockedStores_) {
        if (store->CheckLockAndMaybeMigrateRow(
            key,
            transaction,
            ERowLockMode::Write,
            Tablet_->GetActiveStore()))
        {
            CheckForUnlockedStore(store);
            break;
        }
    }

    // TODO(babenko): check passive stores for write timestamps
}

void TStoreManager::CheckForUnlockedStore(const TDynamicMemoryStorePtr& store)
{
    if (store == Tablet_->GetActiveStore() || store->GetLockCount() > 0)
        return;

    LOG_INFO("Store unlocked and will be dropped (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
    YCHECK(LockedStores_.erase(store) == 1);
}

bool TStoreManager::IsRotationNeeded() const
{
    const auto& store = Tablet_->GetActiveStore();
    return
        store->GetKeyCount() >= Config_->KeyCountRotationThreshold ||
        store->GetValueCount() >= Config_->ValueCountRotationThreshold ||
        store->GetStringSpace() >= Config_->StringSpaceRotationThreshold;
}

void TStoreManager::SetRotationScheduled()
{
    RotationScheduled_ = true;

    LOG_INFO("Tablet store rotation scheduled (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
}

void TStoreManager::ResetRotationScheduled()
{
    if (RotationScheduled_) {
        RotationScheduled_ = false;
        LOG_INFO("Tablet store rotation canceled (TabletId: %s)",
            ~ToString(Tablet_->GetId()));
    }
}

void TStoreManager::Rotate(bool createNew)
{
    RotationScheduled_ = false;

    auto activeStore = Tablet_->GetActiveStore();
    YCHECK(activeStore);
    activeStore->SetState(EStoreState::PassiveDynamic);

    if (activeStore->GetLockCount() > 0) {
        LOG_INFO("Current store is locked and will be kept (TabletId: %s, StoreId: %s, LockCount: %d)",
            ~ToString(Tablet_->GetId()),
            ~ToString(activeStore->GetId()),
            activeStore->GetLockCount());
        YCHECK(LockedStores_.insert(activeStore).second);
    }

    if (createNew) {
        CreateActiveStore();
    } else {
        Tablet_->SetActiveStore(nullptr);
    }

    LOG_INFO("Tablet stores rotated (TabletId: %s, StoreCount: %d)",
        ~ToString(Tablet_->GetId()),
        static_cast<int>(Tablet_->Stores().size()));
}

void TStoreManager::CreateActiveStore()
{
    auto* slot = Tablet_->GetSlot();
    // NB: For tests mostly.
    auto id = slot ? slot->GenerateId(EObjectType::DynamicMemoryTabletStore) : TStoreId::Create();
 
    auto store = New<TDynamicMemoryStore>(
        Config_,
        id,
        Tablet_);

    Tablet_->AddStore(store);
    Tablet_->SetActiveStore(store);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
