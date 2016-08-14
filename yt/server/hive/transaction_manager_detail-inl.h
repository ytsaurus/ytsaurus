#ifndef TRANSACTION_MANAGER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager_detail.h"
#endif

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
TTransaction*TTransactionManagerBase<TTransaction>:: FindPersistentTransaction(const TTransactionId& transactionId)
{
    return PersistentTransactionMap_.Find(transactionId);
}

template <class TTransaction>
TTransaction* TTransactionManagerBase<TTransaction>::GetPersistentTransaction(const TTransactionId& transactionId)
{
    return PersistentTransactionMap_.Get(transactionId);
}

template <class TTransaction>
TTransaction* TTransactionManagerBase<TTransaction>::GetPersistentTransactionOrThrow(const TTransactionId& transactionId)
{
    if (auto* transaction = PersistentTransactionMap_.Find(transactionId)) {
        return transaction;
    }
    THROW_ERROR_EXCEPTION(
        NTransactionClient::EErrorCode::NoSuchTransaction,
        "No such transaction %v",
        transactionId);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterAction(
    const TTransactionId& transactionId,
    TTimestamp transactionStartTimestamp,
    TDuration transactionTimeout,
    const TTransactionActionData& data)
{
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterPrepareActionHandler(
    const TTransactionPrepareActionHandlerDescriptor& descriptor)
{
    YCHECK(PrepareActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterCommitActionHandler(
    const TTransactionCommitActionHandlerDescriptor& descriptor)
{
    YCHECK(CommitActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterAbortActionHandler(
    const TTransactionAbortActionHandlerDescriptor& descriptor)
{
    YCHECK(AbortActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

template <class TTransaction>
TTransactionManagerBase<TTransaction>::TTransactionManagerBase(
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : TCompositeAutomatonPart(
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker))
{ }

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunPrepareTransactionActions(
    TTransaction* transaction,
    bool persistent)
{
    for (const auto& action : transaction->Actions()) {
        auto it = PrepareActionHandlerMap_.find(action.Type);
        if (it == PrepareActionHandlerMap_.end()) {
            continue;
        }
        LOG_DEBUG_UNLESS(IsRecovery(), "Running prepare action handler (TransactionId: %v, ActionType: %v, Persistent: %v)",
            transaction->GetId(),
            action.Type,
            persistent);
        it->second.Run(action.Value, persistent);
    }
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunCommitTransactionActions(TTransaction* transaction)
{
    for (const auto& action : transaction->Actions()) {
        auto it = CommitActionHandlerMap_.find(action.Type);
        if (it == CommitActionHandlerMap_.end()) {
            continue;
        }
        LOG_DEBUG_UNLESS(IsRecovery(), "Running commit action handler (TransactionId: %v, ActionType: %v)",
            transaction->GetId(),
            action.Type);
        it->second.Run(action.Value);
    }
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunAbortTransactionActions(TTransaction* transaction)
{
    for (const auto& action : transaction->Actions()) {
        auto it = AbortActionHandlerMap_.find(action.Type);
        if (it == AbortActionHandlerMap_.end()) {
            continue;
        }
        LOG_DEBUG_UNLESS(IsRecovery(), "Running abort action handler (TransactionId: %v, ActionType: %v)",
            transaction->GetId(),
            action.Type);
        it->second.Run(action.Value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
