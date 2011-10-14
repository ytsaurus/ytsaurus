#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_holder/common.h"
#include "../cypress/common.h"

namespace NYT {
namespace NTransaction {

using NCypress::TNodeId;
using NCypress::TLockId;

////////////////////////////////////////////////////////////////////////////////

class TTransaction
{
    DECLARE_BYREF_RW_PROPERTY(AddedChunkIds, yvector<TChunkId>);
    DECLARE_BYREF_RW_PROPERTY(LockIds, yvector<TLockId>);
    DECLARE_BYREF_RW_PROPERTY(BranchedNodeIds, yvector<TNodeId>);
    DECLARE_BYREF_RW_PROPERTY(CreatedNodeIds, yvector<TNodeId>);

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

private:
    TTransactionId Id;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
