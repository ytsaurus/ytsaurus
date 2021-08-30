#pragma once

#include <yt/yt/core/misc/enum.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeStrategy,
    (Default)
    (Overwrite)
    (Combine)
);

DEFINE_ENUM(EUnrecognizedStrategy,
    (Drop)
    (Keep)
    (KeepRecursive)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
