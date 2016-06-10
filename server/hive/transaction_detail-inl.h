#ifndef TRANSACTION_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_detail-inl.h"
#endif

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TTransactionBase<TBase>::TTransactionBase(const TTransactionId& id)
    : TBase(id)
    , State_(ETransactionState::Active)
{ }

template <class TBase>
ETransactionState TTransactionBase<TBase>::GetPersistentState() const
{
    switch (this->State_) {
        case ETransactionState::TransientCommitPrepared:
        case ETransactionState::TransientAbortPrepared:
            return ETransactionState::Active;
        default:
            return this->State_;
    }
}

template <class TBase>
void TTransactionBase<TBase>::ThrowInvalidState() const
{
    THROW_ERROR_EXCEPTION("Transaction %v is in %Qlv state",
        this->Id_,
        this->State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
