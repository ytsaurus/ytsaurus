#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
class TLock
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TLockId);
    DECLARE_BYVAL_RO_PROPERTY(NodeId, TNodeId);
    DECLARE_BYVAL_RO_PROPERTY(TransactionId, TTransactionId);
    DECLARE_BYVAL_RO_PROPERTY(Mode, ELockMode);

public:
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        ELockMode mode)
        : Id_(id)
        , NodeId_(nodeId)
        , TransactionId_(transactionId)
        , Mode_(mode)
    { }

    TAutoPtr<TLock> Clone() const
    {
        return new TLock(*this);
    }

private:
    TLock(const TLock& other)
        : Id_(other.Id_)
        , NodeId_(other.NodeId_)
        , TransactionId_(other.TransactionId_)
        , Mode_(other.Mode_)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

