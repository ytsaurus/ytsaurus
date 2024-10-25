#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

namespace NYT::NHydra {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options)
{
    auto selfId = cellManager->GetSelfPeerId();
    auto voting = cellManager->GetPeerConfig(selfId)->Voting;
    return voting || options.EnableObserverPersistence;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
