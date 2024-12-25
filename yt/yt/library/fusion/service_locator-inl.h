#ifndef SERVICE_LOCATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include service_locator.h"
// For the sake of sane code completion
#include "service_locator.h"
#endif

#include "traits.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

template <class TServicePtr>
TServicePtr IServiceLocator::FindService()
{
    auto* untypedService = FindUntypedService(NDetail::TServiceIdTraits<TServicePtr>::Id);
    if (!untypedService) {
        return {};
    }
    using TUnderlying = typename NDetail::TServiceIdTraits<TServicePtr>::TUnderlying;
    return TServicePtr(static_cast<TUnderlying*>(untypedService));
}

template <class TServicePtr>
TServicePtr IServiceLocator::GetServiceOrThrow()
{
    auto service = FindService<TServicePtr>();
    if (!service) {
        THROW_ERROR_EXCEPTION("Service %v is not registered",
            NDetail::TServiceIdTraits<TServicePtr>::Id);
    }
    return service;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion
