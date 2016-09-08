#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>
#include <yt/server/hydra/composite_automaton.h>

#include "helpers.h"

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
class TTransactionManagerBase
    : public virtual NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
public:
    TTransaction* FindPersistentTransaction(const TTransactionId& transactionId);
    TTransaction* GetPersistentTransaction(const TTransactionId& transactionId);
    TTransaction* GetPersistentTransactionOrThrow(const TTransactionId& transactionId);

    void RegisterAction(
        const TTransactionId& transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        const TTransactionActionData& data);
    void RegisterPrepareActionHandler(const TTransactionPrepareActionHandlerDescriptor<TTransaction>& descriptor);
    void RegisterCommitActionHandler(const TTransactionCommitActionHandlerDescriptor<TTransaction>& descriptor);
    void RegisterAbortActionHandler(const TTransactionAbortActionHandlerDescriptor<TTransaction>& descriptor);

protected:
    NHydra::TEntityMap<TTransaction> PersistentTransactionMap_;

    yhash_map<Stroka, TTransactionPrepareActionHandler<TTransaction>> PrepareActionHandlerMap_;
    yhash_map<Stroka, TTransactionCommitActionHandler<TTransaction>> CommitActionHandlerMap_;
    yhash_map<Stroka, TTransactionAbortActionHandler<TTransaction>> AbortActionHandlerMap_;


    TTransactionManagerBase(
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);

    void RunPrepareTransactionActions(TTransaction* transaction, bool persistent);
    void RunCommitTransactionActions(TTransaction* transaction);
    void RunAbortTransactionActions(TTransaction* transaction);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT

#define TRANSACTION_MANAGER_DETAIL_INL_H_
#include "transaction_manager_detail-inl.h"
#undef TRANSACTION_MANAGER_DETAIL_INL_H_
