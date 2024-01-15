#ifndef MAINTENANCE_TARGET_INL_H_
#error "Direct inclusion of this file is not allowed, include maintenance_target.h"
// For the sake of sane code completion.
#include "maintenance_target.h"
#endif

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl, EMaintenanceType... Types>
NObjectServer::TObject* TMaintenanceTarget<TImpl, Types...>::AsObject()
{
    static_assert(
        std::derived_from<TImpl, NObjectServer::TObject>,
        "TImpl argument of TMaintenanceTarget template must be a subclass of NObjectServer::TObject");

    return static_cast<TImpl*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
