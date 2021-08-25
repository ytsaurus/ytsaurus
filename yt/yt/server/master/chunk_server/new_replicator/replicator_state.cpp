#include "replicator_state.h"

namespace NYT::NChunkServer::NReplicator {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TReplicatorState
    : public IReplicatorState
{
public:
    explicit TReplicatorState(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        Y_UNUSED(Bootstrap_);
    }

    virtual void Load()
    { }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

IReplicatorStatePtr CreateReplicatorState(TBootstrap* bootstrap)
{
    return New<TReplicatorState>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
