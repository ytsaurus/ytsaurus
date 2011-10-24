#pragma once

#include "../misc/common.h"
#include "../misc/common.h"
#include "../misc/guid.h"

#include "../actions/invoker.h"
#include "../actions/action.h"
#include "../actions/future.h"
#include "../actions/action_util.h"

#include "../logging/log.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TSessionId;
typedef i64 TSequenceId;

extern NLog::TLogger BusLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

