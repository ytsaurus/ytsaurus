#pragma once

#include "common.h"

#include "../chunk_holder/common.h"
#include "../cypress/common.h"

namespace NYT {
namespace NTransaction {

using NCypress::TNodeId;
using NCypress::TLockId;

////////////////////////////////////////////////////////////////////////////////

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
        YUNIMPLEMENTED();
        // output >> Id >> AddedChunkIds_ >> LockIds_ >> BranchedNodeIds_; // is it correct?
    }

    static TAutoPtr<TTransaction> Load(TInputStream* input)
    {
        YUNIMPLEMENTED();
        //TTransactionId id;
        //yvector<TChunkId> addedChunkIds;
        //yvector<TLockId> lockIds;
        //yvector<TNodeId> branchedNodeIds;
        //*input >> id >> addedChunkIds >> lockIds >> branchedNodeIds;
        //return new TTransaction(id); // and what about vectors?
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

    yvector<TNodeId>& CreatedNodeIds()
    {
        return CreatedNodeIds_;
    }

private:
    TTransactionId Id;
    yvector<TChunkId> AddedChunkIds_;
    yvector<TLockId> LockIds_;
    yvector<TNodeId> BranchedNodeIds_;
    yvector<TNodeId> CreatedNodeIds_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
