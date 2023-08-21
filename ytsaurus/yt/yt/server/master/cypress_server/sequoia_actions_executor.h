#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaActionsExecutor
    : public TRefCounted
{
    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaActionsExecutor)

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
