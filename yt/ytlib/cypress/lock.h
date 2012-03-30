#pragma once

#include "id.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/object_server/object_detail.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TLock
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RO_PROPERTY(TNodeId, NodeId);
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, TransactionId);
    DEFINE_BYVAL_RO_PROPERTY(ELockMode, Mode);

public:
    TLock(const TLockId& id);
    
    TLock(
        const TLockId& id,
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        ELockMode mode);

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

