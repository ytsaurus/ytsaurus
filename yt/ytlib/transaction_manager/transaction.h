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
    DECLARE_BYREF_RW_PROPERTY(AddedChunkIds, yvector<TChunkId>);
    DECLARE_BYREF_RW_PROPERTY(LockIds, yvector<TLockId>);
    DECLARE_BYREF_RW_PROPERTY(BranchedNodeIds, yvector<TNodeId>);
    DECLARE_BYREF_RW_PROPERTY(CreatedNodeIds, yvector<TNodeId>);

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

private:
    TTransaction(const TTransaction& other)
        : Id_(other.Id_)
        , AddedChunkIds_(other.AddedChunkIds_)
        , LockIds_(other.LockIds_)
        , BranchedNodeIds_(other.BranchedNodeIds_)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
