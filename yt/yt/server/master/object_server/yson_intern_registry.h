#pragma once

#include "public.h"

#include <yt/core/yson/string.h>

#include <yt/core/misc/serialize.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IYsonInternRegistry
    : public TRefCounted
{
    virtual NYson::TYsonString Intern(NYson::TYsonString value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYsonInternRegistry)

IYsonInternRegistryPtr CreateYsonInternRegistry();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

