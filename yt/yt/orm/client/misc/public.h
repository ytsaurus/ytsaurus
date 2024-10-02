#pragma once

#include <yt/yt/orm/client/misc/autogen/error.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NOrm::NClient {

////////////////////////////////////////////////////////////////////////////////

enum EOptionalBoolean : char
{
    OB_UNKNOWN = 'u',
    OB_TRUE = 't',
    OB_FALSE = 'f',
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient
