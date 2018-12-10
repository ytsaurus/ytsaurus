#pragma once

#include "transaction_manager.h"

#include <yt/ytlib/transaction_client/action.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TTransactionBase
    : public TBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTransactionActionData>, Actions);

public:
    explicit TTransactionBase(const TTransactionId& id);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    ETransactionState GetPersistentState() const;

    void ThrowInvalidState() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer

#define TRANSACTION_DETAIL_INL_H_
#include "transaction_detail-inl.h"
#undef TRANSACTION_DETAIL_INL_H_
