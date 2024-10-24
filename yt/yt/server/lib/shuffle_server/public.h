#pragma once

#include <yt/yt/core/misc/guid.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IShuffleManager);
DECLARE_REFCOUNTED_STRUCT(IShuffleController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
