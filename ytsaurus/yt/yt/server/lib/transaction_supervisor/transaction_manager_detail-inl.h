#ifndef TRANSACTION_MANAGER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager_detail.h"
// For the sake of sane code completion.
#include "transaction_manager_detail.h"
#endif

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterTransactionActionHandlers(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor)
{
    EmplaceOrCrash(PrepareActionHandlerMap_, prepareActionDescriptor.Type, prepareActionDescriptor.Handler);
    EmplaceOrCrash(CommitActionHandlerMap_, commitActionDescriptor.Type, commitActionDescriptor.Handler);
    EmplaceOrCrash(AbortActionHandlerMap_, abortActionDescriptor.Type, abortActionDescriptor.Handler);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RegisterTransactionActionHandlers(
    const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
    const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
    const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor,
    const TTransactionSerializeActionHandlerDescriptor<TTransaction>& serializeActionDescriptor)
{
    RegisterTransactionActionHandlers(
        prepareActionDescriptor,
        commitActionDescriptor,
        abortActionDescriptor);
    EmplaceOrCrash(SerializeActionHandlerMap_, serializeActionDescriptor.Type, serializeActionDescriptor.Handler);
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunPrepareTransactionActions(
    TTransaction* transaction,
    const TTransactionPrepareOptions& options)
{
    for (const auto& action : transaction->Actions()) {
        try {
            auto it = PrepareActionHandlerMap_.find(action.Type);
            if (it == PrepareActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            it->second(transaction, action.Value, options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Prepare action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
            throw;
        }
    }
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunCommitTransactionActions(
    TTransaction* transaction,
    const TTransactionCommitOptions& options)
{
    for (const auto& action : transaction->Actions()) {
        try {
            auto it = CommitActionHandlerMap_.find(action.Type);
            if (it == CommitActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            it->second(transaction, action.Value, options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Commit action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
    }
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunAbortTransactionActions(
    TTransaction* transaction,
    const TTransactionAbortOptions& options)
{
    for (const auto& action : transaction->Actions()) {
        try {
            auto it = AbortActionHandlerMap_.find(action.Type);
            if (it == AbortActionHandlerMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            it->second(transaction, action.Value, options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Abort action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
    }
}

template <class TTransaction>
void TTransactionManagerBase<TTransaction>::RunSerializeTransactionActions(TTransaction* transaction)
{
    for (const auto& action : transaction->Actions()) {
        try {
            if (auto it = SerializeActionHandlerMap_.find(action.Type)) {
                it->second(transaction, action.Value);
            }
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Serialize action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
