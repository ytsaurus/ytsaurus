#ifndef CELL_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include cell_manager.h"
#endif
#undef CELL_MANAGER_INL_H_

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TProxy>
TAutoPtr<TProxy> TCellManager::GetMasterProxy(TPeerId id) const
{
    return new TProxy(~ChannelCache.GetChannel(Config->Addresses.at(id)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NMetaState
