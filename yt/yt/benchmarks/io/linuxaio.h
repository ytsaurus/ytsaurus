#pragma once

#include <util/system/error.h>
#include <util/datetime/base.h>

#include <linux/aio_abi.h>
#include <sys/syscall.h>

#include <vector>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

using TLinuxAioContext = aio_context_t;
using TLinuxAioControlBlock = struct iocb;
using TLinuxAioEvent = struct io_event;

TLinuxAioContext LinuxAioCreate(unsigned queueSize);

void LinuxAioDestroy(TLinuxAioContext context);

int LinuxAioSubmit(TLinuxAioContext context, const std::vector<TLinuxAioControlBlock*> controlBlocks);

int LinuxAioGetEvents(TLinuxAioContext context, int minEvents, TDuration timeout, std::vector<TLinuxAioEvent>* events);

int LinuxAioGetEventsInUserspace(TLinuxAioContext context, std::vector<TLinuxAioEvent>* events);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
