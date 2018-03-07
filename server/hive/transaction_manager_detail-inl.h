#pragma once
#ifndef TRANSACTION_MANAGER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager_detail.h"
#endif

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterPrepareActionHandler(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& descriptor)
{
    YCHECK(PrepareActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterCommitActionHandler(
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& descriptor)
{
    YCHECK(CommitActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterAbortActionHandler(
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& descriptor)
{
    YCHECK(AbortActionHandlerMap_.emplace(descriptor.Type, descriptor.Handler).second);
}

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
        try {
            it->second.Run(transaction, action.Value, persistent);
        } catch (const std::exception& ex) {
            LOG_DEBUG(ex, "Prepare action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
            throw;
        }
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
        try {
            it->second.Run(transaction, action.Value);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Unexpected error: commit action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
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
        try {
            it->second.Run(transaction, action.Value);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Unexpected error: abort action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
