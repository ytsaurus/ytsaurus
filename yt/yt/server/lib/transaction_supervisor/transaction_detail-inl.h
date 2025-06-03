#ifndef TRANSACTION_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_detail.h"
// For the sake of sane code completion.
#include "transaction_detail.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TSaveContext, class TLoadContext>
TTransactionBase<TBase, TSaveContext, TLoadContext>::TAction::TAction(
    NTransactionClient::TTransactionActionData&& data)
{
    *static_cast<TTransactionActionData*>(this) = std::move(data);
}

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TSaveContext, class TLoadContext>
ETransactionState TTransactionBase<TBase, TSaveContext, TLoadContext>::GetPersistentState() const
{
    switch (State_) {
        case ETransactionState::TransientCommitPrepared:
        case ETransactionState::TransientAbortPrepared:
            return ETransactionState::Active;
        default:
            return State_;
    }
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::SetPersistentState(ETransactionState state)
{
    YT_VERIFY(
        state == ETransactionState::Active ||
        state == ETransactionState::PersistentCommitPrepared ||
        state == ETransactionState::CommitPending ||
        state == ETransactionState::Committed ||
        state == ETransactionState::Serialized ||
        state == ETransactionState::Aborted);
    State_ = state;
}

template <class TBase, class TSaveContext, class TLoadContext>
ETransactionState TTransactionBase<TBase, TSaveContext, TLoadContext>::GetTransientState() const
{
    return State_;
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::SetTransientState(ETransactionState state)
{
    YT_VERIFY(
        state == ETransactionState::TransientCommitPrepared ||
        state == ETransactionState::TransientAbortPrepared);
    State_ = state;
}

template <class TBase, class TSaveContext, class TLoadContext>
ETransactionState TTransactionBase<TBase, TSaveContext, TLoadContext>::GetState(bool persistent) const
{
    return persistent ? GetPersistentState() : GetTransientState();
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::ResetTransientState()
{
    auto persistentState = GetPersistentState();
    // Also resets transient state.
    SetPersistentState(persistentState);
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::ThrowInvalidState() const
{
    THROW_ERROR_EXCEPTION(
        NTransactionClient::EErrorCode::InvalidTransactionState,
        "Transaction %v is in %Qlv state",
        this->Id_,
        State_);
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::Save(TSaveContext& context) const
{
    using NYT::Save;

    TSizeSerializer::Save(context, Actions_.size());
    for (const auto& action : Actions_) {
        Save(context, action.Type);
        Save(context, action.Value);
        if (action.State) {
            Save(context, true);
            action.State->Save(context);
        } else {
            Save(context, false);
        }
    }

    Save(context, PreparedActionCount_);
}

template <class TBase, class TSaveContext, class TLoadContext>
void TTransactionBase<TBase, TSaveContext, TLoadContext>::Load(TLoadContext& context)
{
    using NYT::Load;

    // COMPAT(kvk1920, babenko)
    constexpr int ChaosReignBase = 300000;
    constexpr int ChaosReignSaneTxActionAbortFix = 300014;
    constexpr int ChaosReignTransactionActionStates = 300104;
    constexpr int TabletReignBase = 100000;
    constexpr int TabletReignTransactionActionStates = 101208;
    constexpr int MasterReignSaneTxActionAbortFix = 2528;
    constexpr int MasterReignTransactionActionStates = 2930;

    bool hasPreparedActionCount;
    bool hasActionStates;
    int version = ToUnderlying(context.GetVersion());

    if (version > ChaosReignBase) {
        hasPreparedActionCount = version >= ChaosReignSaneTxActionAbortFix;
        hasActionStates = version >= ChaosReignTransactionActionStates;
    } else if (version > TabletReignBase) {
        hasPreparedActionCount = true;
        hasActionStates = version >= TabletReignTransactionActionStates;
    } else {
        hasPreparedActionCount = version >= MasterReignSaneTxActionAbortFix;
        hasActionStates = version >= MasterReignTransactionActionStates;
    }

    {
        size_t actionCount = TSizeSerializer::Load(context);
        for (size_t index =  0; index < actionCount; ++index) {
            auto& action = Actions_.emplace_back();
            Load(context, action.Type);
            Load(context, action.Value);
            if (hasActionStates) {
                if (Load<bool>(context)) {
                    action.State = GetActionStateFactory()->CreateTransactionActionState(action.Type);
                    action.State->Load(context);
                }
            }
        }
    }

    if (hasPreparedActionCount) {
        Load(context, PreparedActionCount_);
    }
}

template <class TBase, class TSaveContext, class TLoadContext>
auto TTransactionBase<TBase, TSaveContext, TLoadContext>::GetActionStateFactory()
    -> IActionStateFactory*
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
