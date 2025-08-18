#pragma once

#include "error.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NOrm::NClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EOptionalBoolean, char,
    ((OB_UNKNOWN) ('u'))
    ((OB_TRUE) ('t'))
    ((OB_FALSE) ('f'))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient
