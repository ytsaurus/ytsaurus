#include "sequoia_actions_executor.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TSequoiaActionsExecutor
    : public ISequoiaActionsExecutor
{
public:
    void Initialize() final
    { }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaActionsExecutorPtr CreateSequoiaActionsExecutor(NCellMaster::TBootstrap* /*bootstrap*/)
{
    return New<TSequoiaActionsExecutor>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
