#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TLock
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TLockId);
    DECLARE_BYVAL_RO_PROPERTY(NodeId, TNodeId);
    DECLARE_BYVAL_RO_PROPERTY(TransactionId, NTransaction::TTransactionId);
    DECLARE_BYVAL_RO_PROPERTY(Mode, ELockMode);

public:
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        const NTransaction::TTransactionId& transactionId,
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

    void Save(TOutputStream* output) const
    {
        ::Save(output, NodeId_);
        ::Save(output, TransactionId_);
        ::Save(output, Mode_);
    }

    static TAutoPtr<TLock> Load(const TLockId& id, TInputStream* input)
    {
        TNodeId nodeId;
        NTransaction::TTransactionId transactionId;
        ELockMode mode;
        ::Load(input, nodeId);
        ::Load(input, transactionId);
        ::Load(input, mode);
        return new TLock(
            id,
            nodeId,
            transactionId,
            mode);
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

