#ifndef TRANSACTION_MANAGER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager_detail.h"
// For the sake of sane code completion.
#include "transaction_manager_detail.h"
#endif

#include "transaction_manager.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::RegisterTransactionActionHandlers(
    TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext> descriptor)
{
    auto type = descriptor.Type();
    EmplaceOrCrash(ActionDescriptorMap_, type, std::move(descriptor));
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::RunPrepareTransactionActions(
    TTransaction* transaction,
    const TTransactionPrepareOptions& options)
{
    // We don't need to run abort tx actions for transient prepare.
    auto rememberPreparedTransactionActionCount = options.Persistent;

    if (rememberPreparedTransactionActionCount) {
        transaction->SetPreparedActionCount(0);
    }

    // It is _not_ just a fast path. The reason of this early return is to avoid
    // nested transaction action check inside |TTransactionActionGuard|.
    // NB: This early return _cannot_ be moved several lines upper because we
    // have to set prepared action count to zero.
    if (transaction->Actions().empty()) {
        return;
    }

    TTransactionActionGuard transactionActionGuard;

    for (int index = 0; index < std::ssize(transaction->Actions()); ++index) {
        auto& action = transaction->Actions()[index];
        try {
            auto it = ActionDescriptorMap_.find(action.Type);
            if (it == ActionDescriptorMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            const auto& descriptor = it->second;
            auto* state = GetOrCreateTransactionActionState(&action, descriptor);
            descriptor.Prepare(transaction, action.Value, state, options);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Prepare action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
            throw;
        }

        if (rememberPreparedTransactionActionCount) {
            transaction->SetPreparedActionCount(*transaction->GetPreparedActionCount() + 1);
        }
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::RunCommitTransactionActions(
    TTransaction* transaction,
    const TTransactionCommitOptions& options)
{
    // See RunPrepareTransactionActions().
    if (transaction->Actions().empty()) {
        return;
    }

    TTransactionActionGuard transactionActionGuard;
    for (int index = 0; index < std::ssize(transaction->Actions()); ++index) {
        auto& action = transaction->Actions()[index];
        bool needDestroyState = false;
        try {
            auto it = ActionDescriptorMap_.find(action.Type);
            if (it == ActionDescriptorMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            const auto& descriptor = it->second;
            needDestroyState = !descriptor.HasSerializeHandler();
            auto* state = GetOrCreateTransactionActionState(&action, descriptor);
            descriptor.Commit(transaction, action.Value, state, options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Commit action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
        if (needDestroyState) {
            // After transaction is finished its side effects should become
            // observable immediately.
            action.State.reset();
        }
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::RunAbortTransactionActions(
    TTransaction* transaction,
    const TTransactionAbortOptions& options,
    bool requireLegacyBehavior)
{
    // See RunPrepareTransactionActions().
    if (transaction->Actions().empty()) {
        return;
    }

    TTransactionActionGuard transactionActionGuard;

    auto runAbort = [&] (int index) {
        auto& action = transaction->Actions()[index];
        try {
            auto it = ActionDescriptorMap_.find(action.Type);
            if (it == ActionDescriptorMap_.end()) {
                THROW_ERROR_EXCEPTION("Action %Qv is not registered",
                    action.Type);
            }
            const auto& descriptor = it->second;
            auto* state = GetOrCreateTransactionActionState(&action, descriptor);
            descriptor.Abort(transaction, action.Value, state, options);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Abort action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
        // After transaction is finished its side effects should become
        // observable immediately.
        action.State.reset();
    };

    // COMPAT(kvk1920)
    if (!transaction->GetPreparedActionCount().has_value() && requireLegacyBehavior) {
        // Previous versions treated |nullopt| as "run all abort actions".The
        // target state is to run nothing (i.e. |nullopt| is semantically equal
        // to 0). This optional<int> should be just int but it cannot be just
        // changed to int due to compatibility reasons.
        for (int index = 0; index < std::ssize(transaction->Actions()); ++index) {
            runAbort(index);
        }
    } else {
        for (int index = transaction->GetPreparedActionCount().value_or(0) - 1; index >= 0; --index) {
            runAbort(index);
        }
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::RunSerializeTransactionActions(TTransaction* transaction)
{
    for (int index = 0; index < std::ssize(transaction->Actions()); ++index) {
        auto& action = transaction->Actions()[index];
        try {
            auto it = ActionDescriptorMap_.find(action.Type);
            if (it == ActionDescriptorMap_.end()) {
                return;
            }
            const auto& descriptor = it->second;
            auto* state = GetOrCreateTransactionActionState(&action, descriptor);
            descriptor.Serialize(transaction, action.Value, state);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Serialize action failed (TransactionId: %v, ActionType: %v)",
                transaction->GetId(),
                action.Type);
        }
        // After transaction is finished its side effects should become
        // observable immediately.
        action.State.reset();
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
auto TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::GetOrCreateTransactionActionState(
    typename TTransaction::TAction* action,
    const TTransactionActionDescriptor& descriptor)
    -> ITransactionActionState*
{
    if (!action->State) {
        action->State = descriptor.CreateState();
    }
    return action->State.get();
}

template <class TTransaction, class TSaveContext, class TLoadContext>
auto TTransactionManagerBase<TTransaction, TSaveContext, TLoadContext>::CreateTransactionActionState(TStringBuf type)
    -> std::unique_ptr<ITransactionActionState>
{
    const auto& descriptor = GetOrCrash(ActionDescriptorMap_, type);
    return descriptor.CreateState();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
