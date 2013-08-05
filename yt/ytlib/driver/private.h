#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct ICommand;
typedef TIntrusivePtr<ICommand> ICommandPtr;

struct ICommandContext;
typedef TIntrusivePtr<ICommandContext> ICommandContextPtr;

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger DriverLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
