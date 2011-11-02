#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../transaction_manager/common.h"

namespace NYT {
namespace NCypress {

using NTransaction::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

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

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id_);
        ::Save(output, NodeId_);
        ::Save(output, TransactionId_);
        // TODO: enum serialization
        ::Save(output, static_cast<i32>(Mode_));
    }

    static TAutoPtr<TLock> Load(TInputStream* input)
    {
        TLockId id;
        TNodeId nodeId;
        TTransactionId transactionId;
        // TODO: enum serialization
        i32 mode;
        ::Load(input, id);
        ::Load(input, nodeId);
        ::Load(input, transactionId);
        ::Load(input, mode);
        return new TLock(
            id,
            nodeId,
            transactionId,
            static_cast<ELockMode>(mode));
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

