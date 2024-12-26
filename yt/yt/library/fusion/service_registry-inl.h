#ifndef SERVICE_REGISTRY_INL_H_
#error "Direct inclusion of this file is not allowed, include service_registry.h"
// For the sake of sane code completion
#include "service_registry.h"
#endif

#include "traits.h"

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

template <class TServicePtr>
void IServiceRegistry::RegisterService(const TServicePtr& service)
{
    RegisterUntypedService(
        NDetail::TServiceIdTraits<TServicePtr>::Id,
        NDetail::TServiceIdTraits<TServicePtr>::ToUntyped(service),
        std::any(service));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion
