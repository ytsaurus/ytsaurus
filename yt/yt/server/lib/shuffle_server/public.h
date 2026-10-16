#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NShuffleServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IShuffleManager);
DECLARE_REFCOUNTED_STRUCT(IShuffleController);
DECLARE_REFCOUNTED_STRUCT(IPullBasedShuffleController);
DECLARE_REFCOUNTED_STRUCT(IPushBasedShuffleController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
