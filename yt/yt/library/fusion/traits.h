#pragma once

#include "service_id.h"

#include <library/cpp/yt/misc/typeid.h>

namespace NYT::NFusion::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TServicePtr>
struct TServiceIdTraits
{ };

template <class TService>
struct TServiceIdTraits<TService*>
{
    using TUnderlying = TService;

    static inline TServiceId Id = TServiceId(Typeid<TService>());

    static void* ToUntyped(TService* service)
    {
        return service;
    }
};

template <class TService>
struct TServiceIdTraits<TIntrusivePtr<TService>>
{
    using TUnderlying = TService;

    static inline TServiceId Id = TServiceId(Typeid<TService>());

    static void* ToUntyped(const TIntrusivePtr<TService>& service)
    {
        return service.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion::NDetail
