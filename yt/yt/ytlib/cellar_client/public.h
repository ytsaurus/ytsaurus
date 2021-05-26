#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NCellarClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECellarType,
    ((Tablet)          (0))
    ((Chaos)           (1))
);

constexpr int TypicalCellarSize = 10;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarClient
