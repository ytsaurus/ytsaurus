#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_holder/common.h"
#include "../cypress/common.h"

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
{
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, Id);
    DEFINE_BYREF_RW_PROPERTY(yvector<NChunkClient::TChunkId>, RegisteredChunks);
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TLockId>, LockIds);
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TNodeId>, BranchedNodes);
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TNodeId>, CreatedNodes);

public:
    TTransaction(const TTransactionId& id);

    TAutoPtr<TTransaction> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TTransaction> Load(const TTransactionId& id, TInputStream* input);

private:
    TTransaction(const TTransaction& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
