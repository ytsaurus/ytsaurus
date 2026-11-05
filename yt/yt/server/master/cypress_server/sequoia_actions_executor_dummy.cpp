#include "sequoia_actions_executor.h"

#include "sequoia_actions_executor_state.h"

namespace NYT::NCypressServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaActionsExecutor
    : public ISequoiaActionsExecutor
    , public TSequoiaActionsExecutorState
{
public:
    explicit TSequoiaActionsExecutor(TBootstrap* bootstrap)
        : TSequoiaActionsExecutorState(bootstrap)
    { }

    void Initialize() final
    { }

    void PrepareSequoiaNodeModification(
        NTransactionServer::TTransaction* /*sequoiaTransaction*/,
        NCypressClient::TVersionedNodeId /*sequoiaNodeId*/,
        NObjectServer::EModificationType /*modificationType*/,
        bool /*latePrepare*/) final
    { }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(NCellMaster::TBootstrap* bootstrap)
{
    return New<TSequoiaActionsExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
