#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TJobId;

typedef TGuid TTaskId;

DECLARE_ENUM(ETaskType,
	((Map)(0))
);

DECLARE_ENUM(EJobType,
	((Map)(0))
);

DECLARE_ENUM(ETaskState,
	((Prepaing)(0))
);

DECLARE_ENUM(EJobState,
	((Running)(0))
	((Completed)(1))
	((Failed)(2))
);

class TSchedulerService;
typedef TIntrusivePtr<TSchedulerService> TSchedulerServicePtr;

class TSchedulerServiceProxy;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
