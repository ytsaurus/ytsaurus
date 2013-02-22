#pragma once

#include <ytlib/scheduler/public.h>

#include <ytlib/misc/enum.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TJobId;

struct TJobProxyConfig;
typedef TIntrusivePtr<TJobProxyConfig> TJobProxyConfigPtr;

class TUserJobIO;

struct IJob;
typedef TIntrusivePtr<IJob> TJobPtr;

struct IJobHost;

struct IDataPipe;
typedef TIntrusivePtr<IDataPipe> IDataPipePtr;

class TErrorOutput;

DECLARE_ENUM(EJobProxyExitCode,
    ((HeartbeatFailed)       (10000))
    ((ResultReportFailed)    (10001))
    ((ResourcesUpdateFailed) (10002))
    ((SetRLimitFailed)       (10003))
    ((ExecFailed)            (10004))
    ((UncaughtException)     (10005))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
