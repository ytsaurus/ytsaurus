#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "automaton.h"
#include "tablet.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "config.h"
#include "store_manager.h"
#include "tablet_cell_controller.h"
#include "private.h"

#include <core/misc/ring_queue.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/tablet_client/config.h>

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
using namespace NTransactionClient;
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
        VERIFY_INVOKER_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        Slot_->GetAutomaton()->RegisterPart(this);

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
        auto transactionManager = Slot_->GetTransactionManager();
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
        const auto& storeManager = tablet->GetStoreManager();
        JustLockedRows_.clear();
        try {
            storeManager->Write(
                transaction,
                chunkMeta,
                blocks,
                true,
                &JustLockedRows_);
        } catch (const std::exception& ex) {
            // Abort just taken locks.
            for (auto row : JustLockedRows_) {
                storeManager->AbortRow(row);
            }
            throw;
        }

        int rowCount = static_cast<int>(JustLockedRows_.size());

        LOG_DEBUG("Rows prewritten (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);

        for (auto row : JustLockedRows_) {
            LockedRows_.push(TDynamicRowRef(tablet, row));
        }

        TReqWriteRows request;
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.mutable_chunk_meta()->Swap(&chunkMeta);
        // TODO(babenko): avoid copying
        for (const auto& block : blocks) {
            request.add_blocks(ToString(block));
        }
        CreateMutation(Slot_->GetHydraManager(), Slot_->GetAutomatonInvoker(), request)
            ->SetAction(BIND(&TImpl::HydraLeaderConfirmRows, MakeStrong(this), rowCount))
            ->Commit();

        CheckForMemoryCompaction(storeManager);
    }

    void Delete(
        TTablet* tablet,
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys)
    {
        const auto& storeManager = tablet->GetStoreManager();
        JustLockedRows_.clear();
        try {
            storeManager->Delete(
                transaction,
                keys,
                true,
                &JustLockedRows_);
        } catch (const std::exception& ex) {
            // Abort just taken locks.
            for (auto bucket : JustLockedRows_) {
                storeManager->AbortRow(bucket);
            }
            throw;
        }

        int rowCount = static_cast<int>(JustLockedRows_.size());

        LOG_DEBUG("Rows predeleted (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);

        for (auto bucket : JustLockedRows_) {
            LockedRows_.push(TDynamicRowRef(tablet, bucket));
        }

        TReqDeleteRows request;
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_keys(), keys);
        CreateMutation(Slot_->GetHydraManager(), Slot_->GetAutomatonInvoker(), request)
            ->SetAction(BIND(&TImpl::HydraLeaderConfirmRows, MakeStrong(this), rowCount))
            ->Commit();

        CheckForMemoryCompaction(storeManager);
    }

    void Lookup(
        TTablet* tablet,
        NVersionedTableClient::TKey key,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks)
    {
        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Lookup(
            key,
            timestamp,
            columnFilter,
            chunkMeta,
            blocks);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    TTabletManagerConfigPtr Config_;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;

    std::vector<TDynamicRow> JustLockedRows_; // pooled instance
    TRingQueue<TDynamicRowRef> LockedRows_;


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
        LockedRows_.clear();
    }

    virtual void OnStopLeading()
    {
        LOG_DEBUG("Started aborting prewritten locks");
        while (!LockedRows_.empty()) {
            auto bucketRef = LockedRows_.front();
            LockedRows_.pop();
            const auto& storeManager = bucketRef.Tablet->GetStoreManager();
            storeManager->AbortRow(bucketRef.Row);
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

        auto storeManager = New<TStoreManager>(
            Config_,
            tablet,
            Slot_->GetAutomatonInvoker(),
            Bootstrap_->GetTabletCellController()->GetCompactionInvoker());
        tablet->SetStoreManager(std::move(storeManager));

        auto hiveManager = Slot_->GetHiveManager();

        {
            TReqOnTabletCreated req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot_->GetMasterMailbox(), req);
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

        auto hiveManager = Slot_->GetHiveManager();

        {
            TReqOnTabletRemoved req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot_->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet removed (TabletId: %s)",
            ~ToString(id));
    }


    void HydraLeaderConfirmRows(int rowCount)
    {
        for (int index = 0; index < rowCount; ++index) {
            YASSERT(!LockedRows_.empty());
            auto bucketRef = LockedRows_.front();
            LockedRows_.pop();
            const auto& storeManager = bucketRef.Tablet->GetStoreManager();
            storeManager->ConfirmRow(bucketRef.Row);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows confirmed (RowCount: %d)",
            rowCount);
    }

    void HydraFollowerWriteRows(const TReqWriteRows& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = GetTablet(tabletId);
        const auto& storeManager = tablet->GetStoreManager();

        std::vector<TSharedRef> blocks;
        for (const auto& block : request.blocks()) {
            blocks.push_back(TSharedRef::FromRefNonOwning(TRef::FromString(block)));
        }

        try {
            storeManager->Write(
                transaction,
                request.chunk_meta(),
                std::move(blocks),
                false,
                &JustLockedRows_);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error writing rows");
        }

        int rowCount = static_cast<int>(JustLockedRows_.size());
        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);
    }

    void HydraFollowerDeleteRows(const TReqDeleteRows& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = GetTablet(tabletId);
        const auto& storeManager = tablet->GetStoreManager();

        auto keys = FromProto<NVersionedTableClient::TOwningKey>(request.keys());

        try {
            storeManager->Delete(
                transaction,
                keys,
                false,
                &JustLockedRows_);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error deleting rows");
        }

        int rowCount = static_cast<int>(JustLockedRows_.size());
        LOG_DEBUG_UNLESS(IsRecovery(), "Rows deleted (TransactionId: %s, TabletId: %s, RowCount: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount);
    }


    void OnTransactionPrepared(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& bucketRef : transaction->LockedRows()) {
                const auto& storeManager = bucketRef.Tablet->GetStoreManager();
                storeManager->PrepareRow(bucketRef.Row);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& bucketRef : transaction->LockedRows()) {
                const auto& storeManager = bucketRef.Tablet->GetStoreManager();
                storeManager->CommitRow(bucketRef.Row);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows committed (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& bucketRef : transaction->LockedRows()) {
                const auto& storeManager = bucketRef.Tablet->GetStoreManager();
                storeManager->AbortRow(bucketRef.Row);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }


    void CheckForMemoryCompaction(const TStoreManagerPtr& storeManager)
    {
        if (!IsLeader())
            return;

        if (!storeManager->IsMemoryCompactionNeeded())
            return;

        storeManager->RunMemoryCompaction();
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
