#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../chunk_server/common.h"
#include "../cypress/common.h"

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
{
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, Id);

    // Chunk Server stuff
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NChunkServer::TChunkTreeId>, UnboundChunkTreeIds);

    // Cypress stuff
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TLockId>, LockIds);
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TNodeId>, BranchedNodeIds);
    DEFINE_BYREF_RW_PROPERTY(yvector<NCypress::TNodeId>, CreatedNodeIds);

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
