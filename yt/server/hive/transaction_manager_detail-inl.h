#pragma once
#ifndef TRANSACTION_MANAGER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager_detail.h"
// For the sake of sane code completion
#include "transaction_manager_detail.h"
#endif

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterTransactionActionHandlers(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor)
{
    YCHECK(PrepareActionHandlerMap_.emplace(prepareActionDescriptor.Type, prepareActionDescriptor.Handler).second);
    YCHECK(CommitActionHandlerMap_.emplace(commitActionDescriptor.Type, commitActionDescriptor.Handler).second);
    YCHECK(AbortActionHandlerMap_.emplace(abortActionDescriptor.Type, abortActionDescriptor.Handler).second);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunPrepareTransactionActions(
    TTransaction* transaction,
    bool persistent)
{
    for (const auto& action : transaction->Actions()) {
        try {
            auto it = PrepareActionHandlerMap_.find(action.Type);
            if (it == PrepareActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
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
        try {
            auto it = CommitActionHandlerMap_.find(action.Type);
            if (it == CommitActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
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
        try {
            auto it = AbortActionHandlerMap_.find(action.Type);
            if (it == AbortActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
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
