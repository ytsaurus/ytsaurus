#include "cell_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

NRpc::TChannelCache TCellManager::ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(const TConfig& config)
    : Config(config)
{ }

i32 TCellManager::GetPeerCount() const
{
    return Config.Addresses.ysize();
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
    return Config.Addresses[id];
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
