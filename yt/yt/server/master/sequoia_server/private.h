#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SequoiaServerLogger, "SequoiaServer");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
