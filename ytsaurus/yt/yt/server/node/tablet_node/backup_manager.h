#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IBackupManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void ValidateReplicationTransactionCommit(TTablet* tablet, TTransaction* transaction) = 0;

    virtual void OnReplicatorWriteTransactionFinished(TTablet* tablet) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBackupManager)

////////////////////////////////////////////////////////////////////////////////

IBackupManagerPtr CreateBackupManager(ITabletSlotPtr slot, IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
