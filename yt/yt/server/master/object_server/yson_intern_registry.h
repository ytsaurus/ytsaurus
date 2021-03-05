#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IYsonInternRegistry
    : public TRefCounted
{
    virtual NYson::TYsonString Intern(NYson::TYsonString value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYsonInternRegistry)

IYsonInternRegistryPtr CreateYsonInternRegistry(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

