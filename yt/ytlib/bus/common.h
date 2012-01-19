#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>

#include <ytlib/actions/invoker.h>
#include <ytlib/actions/action.h>
#include <ytlib/actions/future.h>
#include <ytlib/actions/action_util.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TSessionId;
typedef i64 TSequenceId;

extern NLog::TLogger BusLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

