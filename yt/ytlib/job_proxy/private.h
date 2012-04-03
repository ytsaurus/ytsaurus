#pragma once

#include "../misc/common.h"
#include "../misc/configurable.h"
#include "../misc/error.h"

#include "../logging/log.h"

#include <util/generic/ptr.h>
#include <util/stream/base.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSupervisor
} // namespace NYT

