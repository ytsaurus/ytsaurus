#pragma once

#include "public.h"
#include "service_id.h"

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

struct IServiceLocator
    : public virtual TRefCounted
{
    virtual void* FindUntypedService(TServiceId id) = 0;

    template <class TServicePtr>
    TServicePtr FindService();

    template <class TServicePtr>
    TServicePtr GetServiceOrThrow();
};

DEFINE_REFCOUNTED_TYPE(IServiceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion

#define SERVICE_LOCATOR_INL_H_
#include "service_locator-inl.h"
#undef SERVICE_LOCATOR_INL_H_
