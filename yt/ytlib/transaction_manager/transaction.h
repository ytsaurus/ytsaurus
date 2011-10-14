#pragma once

#include "common.h"

#include "../chunk_holder/common.h"
#include "../cypress/common.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NTransaction {

using NCypress::TNodeId;
using NCypress::TLockId;

////////////////////////////////////////////////////////////////////////////////
// TODO: move implementation to cpp

class TTransaction
{
public:
    TTransaction(const TTransaction& other)
        : Id(other.Id)
        , AddedChunkIds_(other.AddedChunkIds_)
        , LockIds_(other.LockIds_)
        , BranchedNodeIds_(other.BranchedNodeIds_)
    { }

    TTransaction(const TTransactionId& id)
        : Id(id)
    { }

    TAutoPtr<TTransaction> Clone() const
    {
        return new TTransaction(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id);
        ::Save(output, AddedChunkIds_);
        ::Save(output, LockIds_);
        ::Save(output, BranchedNodeIds_);
    }

    static TAutoPtr<TTransaction> Load(TInputStream* input)
    {
        TTransactionId id;
        ::Load(input, id);
        auto* transaction = new TTransaction(id);
        ::Load(input, transaction->AddedChunkIds_);
        ::Load(input, transaction->LockIds_);
        ::Load(input, transaction->BranchedNodeIds_);
        return transaction;
    }

    TTransactionId GetId() const
    {
        return Id;
    }

    yvector<TChunkId>& AddedChunkIds()
    {
        return AddedChunkIds_;
    }

    yvector<TLockId>& LockIds()
    {
        return LockIds_;
    }

    yvector<TNodeId>& BranchedNodeIds()
    {
        return BranchedNodeIds_;
    }

private:
    TTransactionId Id;
    yvector<TChunkId> AddedChunkIds_;
    yvector<TLockId> LockIds_;
    yvector<TNodeId> BranchedNodeIds_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
