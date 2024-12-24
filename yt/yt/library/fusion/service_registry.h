#pragma once

#include "public.h"
#include "service_id.h"

#include <any>

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

struct IServiceRegistry
    : public virtual TRefCounted
{
    virtual void RegisterUntypedService(
        TServiceId id,
        void* untypedService,
        std::any serviceHolder) = 0;

    template <class TServicePtr>
    void RegisterService(const TServicePtr& service);
};

DEFINE_REFCOUNTED_TYPE(IServiceRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion

#define SERVICE_REGISTRY_INL_H_
#include "service_registry-inl.h"
#undef SERVICE_REGISTRY_INL_H_
