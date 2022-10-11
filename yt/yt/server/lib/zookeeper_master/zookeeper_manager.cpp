#include "zookeeper_manager.h"

#include "bootstrap.h"
#include "zookeeper_shard.h"

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>
#include <yt/yt/server/lib/hydra_common/hydra_context.h>

namespace NYT::NZookeeperMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperManager
    : public IZookeeperManager
    , public TCompositeAutomatonPart
{
public:
    explicit TZookeeperManager(IBootstrap* bootstrap)
        : TCompositeAutomatonPart(
            bootstrap->GetHydraManager(),
            bootstrap->GetAutomaton(),
            bootstrap->GetAutomatonInvoker())
        , Bootstrap_(bootstrap)
    { }

    void Clear() override
    {
        RootShard_ = nullptr;
    }

    void RegisterShard(TZookeeperShard* shard) override
    {
        YT_VERIFY(HasHydraContext());

        YT_VERIFY(!RootShard_);
        RootShard_ = shard;
    }

    void UnregisterShard(TZookeeperShard* shard) override
    {
        YT_VERIFY(HasHydraContext());

        YT_VERIFY(shard->GetShardId() == RootShard_->GetShardId());
        RootShard_ = nullptr;
    }

    bool HasRootShard() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return RootShard_;
    }

private:
    IBootstrap* const Bootstrap_;

    //! The only possible (for now) shard.
    TZookeeperShard* RootShard_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

IZookeeperManagerPtr CreateZookeeperManager(IBootstrap* bootstrap)
{
    return New<TZookeeperManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
