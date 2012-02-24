#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
{
public:
    explicit TLoadContext(TBootstrap* bootstrap);

    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCellMaster
} // namespace NYT
