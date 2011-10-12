#pragma once

#include "common.h"

#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

class TLock
{
public:
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        ELockMode mode)
        : Id(id)
        , NodeId(nodeId)
        , TransactionId(transactionId)
        , Mode(mode)
    { }

    TLock(const TLock& other)
        : Id(other.Id)
        , NodeId(other.NodeId)
        , TransactionId(other.TransactionId)
        , Mode(other.Mode)
    { }

    TAutoPtr<TLock> Clone() const
    {
        return new TLock(*this);
    }

    TLockId GetId() const
    {
        return Id;
    }

    TNodeId GetNodeId() const
    {
        return NodeId;
    }

    TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    ELockMode GetMode() const
    {
        return Mode;
    }

private:
    TLockId Id;
    TNodeId NodeId;
    TTransactionId TransactionId;
    ELockMode Mode;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

