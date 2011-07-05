#ifndef CELL_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include action_util.h"
#endif
#undef CELL_MANAGER_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TProxy>
TAutoPtr<TProxy> TCellManager::GetMasterProxy(TMasterId id) const
{
    return new TProxy(ChannelCache.GetChannel(Config.MasterAddresses.at(id)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
