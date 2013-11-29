#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "automaton.h"
#include "tablet.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "memory_table.h"
#include "config.h"
#include "private.h"

#include <core/misc/ring_queue.h>

#include <core/concurrency/fiber.h>

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/mutation_context.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NVersionedTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        TTabletSlot* slot,
        TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
    {
        VERIFY_INVOKER_AFFINITY(Slot->GetAutomatonInvoker(), AutomatonThread);

        Slot->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, MakeStrong(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, MakeStrong(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, MakeStrong(this)));

        RegisterMethod(BIND(&TImpl::HydraCreateTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerWriteRows, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerDeleteRows, Unretained(this)));
    }


    void Initialize()
    {
        auto transactionManager = Slot->GetTransactionManager();
        transactionManager->SubscribeTransactionPrepared(BIND(&TImpl::OnTransactionPrepared, MakeStrong(this)));
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionCommitted, MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionAborted, MakeStrong(this)));
    }


    TTablet* GetTabletOrThrow(const TTabletId& id)
    {
        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION("No such tablet %s",
                ~ToString(id));
        }
        return tablet;
    }


    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks)
    {
        JustLockedBuckets_.clear();
        try {
            DoWriteRows(
                tablet,
                transaction,
                chunkMeta,
                blocks,
                true);
        } catch (const std::exception& ex) {
            // Abort just taken locks.
            const auto& memoryTable = tablet->GetActiveMemoryTable();
            for (auto bucket : JustLockedBuckets_) {
                memoryTable->AbortBucket(bucket);
            }
            throw;
        }

        int rowCount = static_cast<int>(JustLockedBuckets_.size());

        LOG_DEBUG("Rows prewritten (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);

        for (auto bucket : JustLockedBuckets_) {
            LockedBuckets_.push(TBucketRef(tablet, bucket));
        }

        TReqWriteRows request;
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.mutable_chunk_meta()->Swap(&chunkMeta);
        // TODO(babenko): avoid copying
        for (const auto& block : blocks) {
            request.add_blocks(ToString(block));
        }
        CreateMutation(Slot->GetHydraManager(), Slot->GetAutomatonInvoker(), request)
            ->SetAction(BIND(&TImpl::HydraLeaderConfirmRows, MakeStrong(this), rowCount))
            ->Commit();
    }

    void Delete(
        TTablet* tablet,
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys)
    {
        JustLockedBuckets_.clear();
        try {
            DoDeleteRows(
                tablet,
                transaction,
                keys,
                true);
        } catch (const std::exception& ex) {
            // Abort just taken locks.
            const auto& memoryTable = tablet->GetActiveMemoryTable();
            for (auto bucket : JustLockedBuckets_) {
                memoryTable->AbortBucket(bucket);
            }
            throw;
        }

        int rowCount = static_cast<int>(JustLockedBuckets_.size());

        LOG_DEBUG("Rows predeleted (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);

        for (auto bucket : JustLockedBuckets_) {
            LockedBuckets_.push(TBucketRef(tablet, bucket));
        }

        TReqDeleteRows request;
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_keys(), keys);
        CreateMutation(Slot->GetHydraManager(), Slot->GetAutomatonInvoker(), request)
            ->SetAction(BIND(&TImpl::HydraLeaderConfirmRows, MakeStrong(this), rowCount))
            ->Commit();
    }

    void Lookup(
        TTablet* tablet,
        NVersionedTableClient::TKey key,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks)
    {
        auto memoryWriter = New<TMemoryWriter>();

        auto chunkWriter = New<TChunkWriter>(
            New<TChunkWriterConfig>(), // TODO(babenko): make static
            New<TEncodingWriterOptions>(), // TODO(babenko): make static
            memoryWriter);

        const auto& memoryTable = tablet->GetActiveMemoryTable();
        memoryTable->LookupRow(
            std::move(chunkWriter),
            key,
            timestamp,
            columnFilter);

        *chunkMeta = std::move(memoryWriter->GetChunkMeta());
        *blocks = std::move(memoryWriter->GetBlocks());
    }



    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    TTabletManagerConfigPtr Config_;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;

    std::vector<TBucket> JustLockedBuckets_; // pooled instance
    TRingQueue<TBucketRef> LockedBuckets_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void SaveKeys(TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        TabletMap_.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadValues(context);
    }

    virtual void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void DoClear()
    {
        TabletMap_.Clear();
    }


    virtual void OnStartLeading()
    {
        LockedBuckets_.clear();
    }

    virtual void OnStopLeading()
    {
        LOG_DEBUG("Started aborting prewritten locks");
        while (!LockedBuckets_.empty()) {
            auto bucketRef = LockedBuckets_.front();
            LockedBuckets_.pop();
            const auto& memoryTable = bucketRef.Tablet->GetActiveMemoryTable();
            memoryTable->AbortBucket(bucketRef.Bucket);
        }
        LOG_DEBUG("Finished aborting prewritten locks");
    }


    void HydraCreateTablet(const TReqCreateTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto schema = FromProto<TTableSchema>(request.schema());
        auto keyColumns = FromProto<Stroka>(request.key_columns().names());

        auto* tablet = new TTablet(
            id,
            schema,
            keyColumns,
            New<TTableMountConfig>());
        TabletMap_.Insert(id, tablet);

        auto memoryTable = New<TMemoryTable>(
            Config_,
            tablet);
        tablet->SetActiveMemoryTable(memoryTable);

        auto hiveManager = Slot->GetHiveManager();

        {
            TReqOnTabletCreated req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TabletId: %s)",
            ~ToString(id));
    }

    void HydraRemoveTablet(const TReqRemoveTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet)
            return;
        
        // TODO(babenko): flush data
        // TODO(babenko): purge prewritten locks

        TabletMap_.Remove(id);

        auto hiveManager = Slot->GetHiveManager();

        {
            TReqOnTabletRemoved req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet removed (TabletId: %s)",
            ~ToString(id));
    }


    void HydraLeaderConfirmRows(int rowCount)
    {
        for (int index = 0; index < rowCount; ++index) {
            YASSERT(!LockedBuckets_.empty());
            auto bucketRef = LockedBuckets_.front();
            LockedBuckets_.pop();
            const auto& memoryTable = bucketRef.Tablet->GetActiveMemoryTable();
            memoryTable->ConfirmBucket(bucketRef.Bucket);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows confirmed (RowCount: %d)",
            rowCount);
    }

    void HydraFollowerWriteRows(const TReqWriteRows& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto transactionManager = Slot->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = GetTablet(tabletId);

        std::vector<TSharedRef> blocks;
        for (const auto& block : request.blocks()) {
            blocks.push_back(TSharedRef::FromRefNonOwning(TRef::FromString(block)));
        }

        try {
            DoWriteRows(
                tablet,
                transaction,
                request.chunk_meta(),
                std::move(blocks),
                false);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error writing rows");
        }

        int rowCount = static_cast<int>(JustLockedBuckets_.size());
        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);
    }

    void HydraFollowerDeleteRows(const TReqDeleteRows& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto transactionManager = Slot->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = GetTablet(tabletId);

        auto keys = FromProto<NVersionedTableClient::TOwningKey>(request.keys());

        try {
            DoDeleteRows(
                tablet,
                transaction,
                keys,
                false);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error deleting rows");
        }

        int rowCount = static_cast<int>(JustLockedBuckets_.size());
        LOG_DEBUG_UNLESS(IsRecovery(), "Rows deleted (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);
    }


    void DoWriteRows(
        TTablet* tablet,
        TTransaction* transaction,
        TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks,
        bool prewrite)
    {
        auto memoryReader = New<TMemoryReader>(
            std::move(chunkMeta),
            std::move(blocks));

        auto chunkReader = CreateChunkReader(
            New<TChunkReaderConfig>(), // TODO(babenko): make configurable or cache this at least
            memoryReader);

        const auto& memoryTable = tablet->GetActiveMemoryTable();

        auto nameTable = New<TNameTable>();

        {
            // The reader is typically synchronous.
            auto error = WaitFor(chunkReader->Open(
                nameTable,
                tablet->Schema(),
                true));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        const int RowsBufferSize = 1000;
        // TODO(babenko): use unversioned rows
        std::vector<TVersionedRow> rows;
        rows.reserve(RowsBufferSize);

        while (true) {
            bool hasData = chunkReader->Read(&rows);
            
            for (auto row : rows) {
                auto bucket = memoryTable->WriteRow(
                    nameTable,
                    transaction,
                    row,
                    prewrite);
                JustLockedBuckets_.push_back(bucket);
            }

            if (!hasData) {
                break;
            }
            
            if (rows.size() < rows.capacity()) {
                // The reader is typically synchronous.
                auto result = WaitFor(chunkReader->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
            
            rows.clear();
        }
    }

    void DoDeleteRows(
        TTablet* tablet,
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys,
        bool predelete)
    {
        const auto& memoryTable = tablet->GetActiveMemoryTable();
        for (auto key : keys) {
            auto bucket = memoryTable->DeleteRow(
                transaction,
                key,
                predelete);
            JustLockedBuckets_.push_back(bucket);
        }
    }


    void OnTransactionPrepared(TTransaction* transaction)
    {
        if (!transaction->LockedBuckets().empty()) {
            for (const auto& bucketRef : transaction->LockedBuckets()) {
                const auto& memoryTable = bucketRef.Tablet->GetActiveMemoryTable();
                memoryTable->PrepareBucket(bucketRef.Bucket);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedBuckets().size());
        }
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        if (!transaction->LockedBuckets().empty()) {
            for (const auto& bucketRef : transaction->LockedBuckets()) {
                const auto& memoryTable = bucketRef.Tablet->GetActiveMemoryTable();
                memoryTable->CommitBucket(bucketRef.Bucket);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows committed (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedBuckets().size());
        }
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (!transaction->LockedBuckets().empty()) {
            for (const auto& bucketRef : transaction->LockedBuckets()) {
                const auto& memoryTable = bucketRef.Tablet->GetActiveMemoryTable();
                memoryTable->AbortBucket(bucketRef.Bucket);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedBuckets().size());
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

TTablet* TTabletManager::GetTabletOrThrow(const TTabletId& id)
{
    return Impl->GetTabletOrThrow(id);
}

void TTabletManager::Initialize()
{
    Impl->Initialize();
}

void TTabletManager::Write(
    TTablet* tablet,
    TTransaction* transaction,
    TChunkMeta chunkMeta,
    std::vector<TSharedRef> blocks)
{
    Impl->Write(
        tablet,
        transaction,
        std::move(chunkMeta),
        std::move(blocks));
}

void TTabletManager::Delete(
    TTablet* tablet,
    TTransaction* transaction,
    const std::vector<NVersionedTableClient::TOwningKey>& keys)
{
    Impl->Delete(
        tablet,
        transaction,
        keys);
}

void TTabletManager::Lookup(
    TTablet* tablet,
    NVersionedTableClient::TKey key,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    TChunkMeta* chunkMeta,
    std::vector<TSharedRef>* blocks)
{
    Impl->Lookup(
        tablet,
        key,
        timestamp,
        columnFilter,
        chunkMeta,
        blocks);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
