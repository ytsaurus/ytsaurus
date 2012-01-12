#pragma once

#include "common.h"
#include "id.h"

#include <yt/ytlib/misc/property.h>
#include <yt/ytlib/cypress/id.h>
#include <yt/ytlib/chunk_server/id.h>
#include <yt/ytlib/object_server/object.h>

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
