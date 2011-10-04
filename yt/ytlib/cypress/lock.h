#pragma once

#include "common.h"

#include "../misc/enum.h"

namespace NYT {
namespace NCypress {

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

