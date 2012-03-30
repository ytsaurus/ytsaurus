#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>

#include <ytlib/actions/invoker.h>
#include <ytlib/actions/bind.h>
#include <ytlib/actions/callback.h>
#include <ytlib/actions/future.h>

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

