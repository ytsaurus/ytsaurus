#include "stdafx.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "config.h"
#include "automaton.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/misc/lease_manager.h>

#include <core/concurrency/thread_affinity.h>

#include <server/hydra/hydra_manager.h>

namespace NYT {
namespace NTabletNode {

using namespace NTransactionClient;
using namespace NHydra;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TTabletAutomatonPart
{
    DEFINE_SIGNAL(void(TTransaction*), TransactionStarted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionCommitted);
    DEFINE_SIGNAL(void(TTransaction*), TransactionAborted);

public:
    TImpl(
        TTransactionManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config(config)
    {
        VERIFY_INVOKER_AFFINITY(Slot->GetAutomatonInvoker(), AutomatonThread);

        Slot->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TransactionManager.Keys",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        RegisterLoader(
            "TransactionManager.Values",
            BIND(&TImpl::LoadValues, MakeStrong(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TransactionManager.Keys",
            BIND(&TImpl::SaveKeys, MakeStrong(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TransactionManager.Values",
            BIND(&TImpl::SaveValues, MakeStrong(this)));
    }


    TTransaction* GetTransactionOrThrow(const TTransactionId& id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        auto* transaction = FindTransaction(id);
        if (!transaction) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such transction %s",
                ~ToString(id));
        }
        return transaction;
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction, TTransactionId);


    // ITransactionManager implementation.
    TTransactionId StartTransaction(
        TTimestamp startTimestamp,
        const NHive::NProto::TReqStartTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return TTransactionId();
    }

    void PrepareTransactionCommit(
        const TTransactionId& transactionId,
        bool persistent,
        TTimestamp prepareTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void CommitTransaction(
        const TTransactionId& transactionId,
        TTimestamp commitTimestamp)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void AbortTransaction(
        const TTransactionId& transactionId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

    void PingTransaction(
        const TTransactionId& transactionId,
        const NHive::NProto::TReqPingTransaction& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

    }

private:
    TTransactionManagerConfigPtr Config;

    TEntityMap<TTransactionId, TTransaction> TransactionMap;
    yhash_map<TTransactionId, TLeaseManager::TLease> LeaseMap;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);



    void SaveKeys(TSaveContext& context)
    {
        TransactionMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context)
    {
        TransactionMap.SaveValues(context);
    }

    void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TransactionMap.LoadValues(context);
    }

    void DoClear()
    {
        TransactionMap.Clear();
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTransactionManager::TImpl, Transaction, TTransaction, TTransactionId, TransactionMap)

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    TTransactionManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTransaction* TTransactionManager::GetTransactionOrThrow(const TTransactionId& id)
{
    return Impl->GetTransactionOrThrow(id);
}

void TTransactionManager::PrepareTransactionCommit(
    const TTransactionId& transactionId,
    bool persistent,
    TTimestamp prepareTimestamp)
{
    Impl->PrepareTransactionCommit(
        transactionId,
        persistent,
        prepareTimestamp);
}

TTransactionId TTransactionManager::StartTransaction(
    TTimestamp startTimestamp,
    const NHive::NProto::TReqStartTransaction& request)
{
    return Impl->StartTransaction(startTimestamp, request);
}

void TTransactionManager::CommitTransaction(
    const TTransactionId& transactionId,
    TTimestamp commitTimestamp)
{
    Impl->CommitTransaction(transactionId, commitTimestamp);
}

void TTransactionManager::AbortTransaction(const TTransactionId& transactionId)
{
    Impl->AbortTransaction(transactionId);
}

void TTransactionManager::PingTransaction(
    const TTransactionId& transactionId,
    const NHive::NProto::TReqPingTransaction& request)
{
    Impl->PingTransaction(transactionId, request);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTransactionManager, Transaction, TTransaction, TTransactionId, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
