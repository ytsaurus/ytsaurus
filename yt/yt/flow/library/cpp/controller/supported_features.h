#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Features supported by this controller build, advertised by get-supported-features.
DEFINE_ENUM(EControllerFeature,
    // Base/effective DynamicSpec views, blame, and versioned patches with cancellation and reset.
    (DynamicSpecPatches)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
