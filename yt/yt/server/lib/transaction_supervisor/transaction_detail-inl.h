#ifndef TRANSACTION_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_detail.h"
// For the sake of sane code completion.
#include "transaction_detail.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
ETransactionState TTransactionBase<TBase>::GetPersistentState() const
{
    switch (State_) {
        case ETransactionState::TransientCommitPrepared:
        case ETransactionState::TransientAbortPrepared:
            return ETransactionState::Active;
        default:
            return State_;
    }
}

template <class TBase>
void TTransactionBase<TBase>::SetPersistentState(ETransactionState state)
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

template <class TBase>
ETransactionState TTransactionBase<TBase>::GetTransientState() const
{
    return State_;
}

template <class TBase>
void TTransactionBase<TBase>::SetTransientState(ETransactionState state)
{
    YT_VERIFY(
        state == ETransactionState::TransientCommitPrepared ||
        state == ETransactionState::TransientAbortPrepared);
    State_ = state;
}

template <class TBase>
ETransactionState TTransactionBase<TBase>::GetState(bool persistent) const
{
    return persistent ? GetPersistentState() : GetTransientState();
}

template <class TBase>
void TTransactionBase<TBase>::ResetTransientState()
{
    auto persistentState = GetPersistentState();
    // Also resets transient state.
    SetPersistentState(persistentState);
}

template <class TBase>
void TTransactionBase<TBase>::ThrowInvalidState() const
{
    THROW_ERROR_EXCEPTION(
        NTransactionClient::EErrorCode::InvalidTransactionState,
        "Transaction %v is in %Qlv state",
        this->Id_,
        State_);
}

template <class TBase>
void TTransactionBase<TBase>::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Actions_);
    Save(context, PreparedActionCount_);
}

template <class TBase>
void TTransactionBase<TBase>::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Actions_);

    // COMPAT(kvk1920)
    constexpr int ChaosReignBase = 300'000;
    constexpr int ChaosReignSaneTxActionAbortFix = 300'014;
    constexpr int TabletReignBase = 100'000;
    constexpr int TabletReignSaneTxActionAbortFix = 100'906;
    constexpr int MasterReignSaneTxActionAbortFix = 2528;

    bool hasPreparedActionCount;
    int version = context.GetVersion();

    if (version > ChaosReignBase) {
        hasPreparedActionCount = version >= ChaosReignSaneTxActionAbortFix;
    } else if (version > TabletReignBase) {
        hasPreparedActionCount = version >= TabletReignSaneTxActionAbortFix;
    } else {
        hasPreparedActionCount = version >= MasterReignSaneTxActionAbortFix;
    }

    if (hasPreparedActionCount) {
        Load(context, PreparedActionCount_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
