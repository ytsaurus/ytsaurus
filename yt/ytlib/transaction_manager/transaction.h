#pragma once

#include "common.h"

#include "../misc/property.h"
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
    DECLARE_BYVAL_RO_PROPERTY(Id, TTransactionId);
    DECLARE_BYREF_RW_PROPERTY(AddedChunks, yvector<TChunkId>);
    DECLARE_BYREF_RW_PROPERTY(Locks, yvector<TLockId>);
    DECLARE_BYREF_RW_PROPERTY(BranchedNodes, yvector<TNodeId>);
    DECLARE_BYREF_RW_PROPERTY(CreatedNodes, yvector<TNodeId>);

public:
    TTransaction(const TTransactionId& id)
        : Id_(id)
    { }

    TAutoPtr<TTransaction> Clone() const
    {
        return new TTransaction(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id_);
        ::Save(output, AddedChunks_);
        ::Save(output, Locks_);
        ::Save(output, BranchedNodes_);
    }

    static TAutoPtr<TTransaction> Load(TInputStream* input)
    {
        TTransactionId id;
        ::Load(input, id);
        auto* transaction = new TTransaction(id);
        ::Load(input, transaction->AddedChunks_);
        ::Load(input, transaction->Locks_);
        ::Load(input, transaction->BranchedNodes_);
        return transaction;
    }

private:
    TTransaction(const TTransaction& other)
        : Id_(other.Id_)
        , AddedChunks_(other.AddedChunks_)
        , Locks_(other.Locks_)
        , BranchedNodes_(other.BranchedNodes_)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
