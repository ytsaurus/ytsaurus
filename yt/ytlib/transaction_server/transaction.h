#pragma once

#include "common.h"
#include "id.h"

#include <ytlib/misc/property.h>
#include <ytlib/cypress/id.h>
#include <ytlib/chunk_server/id.h>
#include <ytlib/object_server/object_detail.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETransactionState,
    (Active)
    (Committed)
    (Aborted)
);

class TTransaction
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TTransactionId>, NestedTransactionIds);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionId, ParentId);

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
