#pragma once

#include <library/cpp/yt/misc/enum.h>

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
