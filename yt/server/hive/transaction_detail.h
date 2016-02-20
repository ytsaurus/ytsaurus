#pragma once

#include "public.h"

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TTransactionBase
    : public TBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);

public:
    explicit TTransactionBase(const TTransactionId& id);

    ETransactionState GetPersistentState() const;

    void ThrowInvalidState() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

#define TRANSACTION_DETAIL_INL_H_
#include "transaction_detail-inl.h"
#undef TRANSACTION_DETAIL_INL_H_
