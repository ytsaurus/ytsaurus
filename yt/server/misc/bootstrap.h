#pragma once

#include "private.h"

#include <yt/core/logging/log.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
{
public:
    TBootstrapBase(
        const NLogging::TLogger& logger,
        const NYTree::TYsonSerializablePtr& config);

protected:
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
