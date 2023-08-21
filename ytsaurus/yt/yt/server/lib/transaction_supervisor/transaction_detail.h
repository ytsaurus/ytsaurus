#pragma once

#include "transaction_manager.h"

#include <yt/yt/ytlib/transaction_client/action.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TTransactionBase
    : public TBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTransactionActionData>, Actions);

public:
    using TBase::TBase;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    ETransactionState GetPersistentState() const;
    void SetPersistentState(ETransactionState state);

    ETransactionState GetTransientState() const;
    void SetTransientState(ETransactionState state);

    ETransactionState GetState(bool persistent) const;

    void ResetTransientState();

    void ThrowInvalidState() const;

private:
    ETransactionState State_ = ETransactionState::Active;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_DETAIL_INL_H_
#include "transaction_detail-inl.h"
#undef TRANSACTION_DETAIL_INL_H_
