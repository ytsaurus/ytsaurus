#pragma once

#include "public.h"
#include "service_locator.h"
#include "service_registry.h"

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

struct IServiceDirectory
    : public IServiceLocator
    , public IServiceRegistry
{ };

DEFINE_REFCOUNTED_TYPE(IServiceDirectory);

////////////////////////////////////////////////////////////////////////////////

IServiceDirectoryPtr CreateServiceDirectory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion
