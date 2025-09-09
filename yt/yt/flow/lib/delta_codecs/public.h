#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)                (0))
    ((XDelta)              (1))
);

DEFINE_ENUM(EAlgorithm,
    ((ForcePatch)        (0))
    ((ZeroPatch)         (1))
    ((SizeHeuristics)    (2))
);

////////////////////////////////////////////////////////////////////////////////

struct ICodec;
struct TState;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
