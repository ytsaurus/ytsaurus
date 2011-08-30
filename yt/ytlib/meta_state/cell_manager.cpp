#include "cell_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(const TConfig& config)
    : Config(config)
{ }

i32 TCellManager::GetPeerCount() const
{
    return Config.PeerAddresses.ysize();
}

i32 TCellManager::GetQuorum() const
{
    return GetPeerCount() / 2 + 1;
}

TPeerId TCellManager::GetSelfId() const
{
    return Config.Id;
}

Stroka TCellManager::GetPeerAddress(TPeerId id) const
{
    return Config.PeerAddresses[id];
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
