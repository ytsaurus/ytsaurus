#pragma once

#include "id.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/transaction_server/public.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TLock
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, NodeId);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    DEFINE_BYVAL_RO_PROPERTY(ELockMode, Mode);

public:
    explicit TLock(const TLockId& id);
    
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction,
        ELockMode mode);

    //! Replaces transaction.
    void PromoteToTransaction(NTransactionServer::TTransaction* transaction);

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

