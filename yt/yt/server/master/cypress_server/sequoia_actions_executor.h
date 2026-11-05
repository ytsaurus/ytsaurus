#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/sequoia_server/revision.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaActionsExecutor
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void PrepareSequoiaNodeModification(
        NTransactionServer::TTransaction* sequoiaTransaction,
        NCypressClient::TVersionedNodeId sequoiaNodeId,
        NObjectServer::EModificationType modificationType,
        bool latePrepare) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaActionsExecutor)

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
