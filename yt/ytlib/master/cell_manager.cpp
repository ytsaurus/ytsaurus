#include "cell_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCellManager::TCellManager(const TConfig& config)
    : Config(config)
{ }

i32 TCellManager::GetMasterCount() const
{
    return Config.MasterAddresses.ysize();
}

i32 TCellManager::GetQuorum() const
{
    return GetMasterCount() / 2 + 1;
}

TMasterId TCellManager::GetSelfId() const
{
    return Config.Id;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
