#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_holder/common.h"
#include "../cypress/common.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NTransaction {

// TODO: get rid
using NCypress::TNodeId;
using NCypress::TLockId;

////////////////////////////////////////////////////////////////////////////////
// TODO: move implementation to cpp

class TTransaction
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TTransactionId);
    DECLARE_BYREF_RW_PROPERTY(RegisteredChunks, yvector<NChunkClient::TChunkId>);
    DECLARE_BYREF_RW_PROPERTY(LockIds, yvector<TLockId>);
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
        YASSERT(output != NULL);

        //::Save(output, Id_);
        ::Save(output, RegisteredChunks_);
        ::Save(output, LockIds_);
        ::Save(output, BranchedNodes_);
    }

    static TAutoPtr<TTransaction> Load(const TTransactionId& id, TInputStream* input)
    {
        YASSERT(input != NULL);

        auto* transaction = new TTransaction(id);
        ::Load(input, transaction->RegisteredChunks_);
        ::Load(input, transaction->LockIds_);
        ::Load(input, transaction->BranchedNodes_);
        return transaction;
    }

private:
    TTransaction(const TTransaction& other)
        : Id_(other.Id_)
        , RegisteredChunks_(other.RegisteredChunks_)
        , LockIds_(other.LockIds_)
        , BranchedNodes_(other.BranchedNodes_)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
