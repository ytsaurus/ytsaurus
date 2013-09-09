#pragma once

#include <ytlib/scheduler/public.h>

#include <core/misc/enum.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyConfig;
typedef TIntrusivePtr<TJobProxyConfig> TJobProxyConfigPtr;

class TUserJobIO;

struct IJob;
typedef TIntrusivePtr<IJob> TJobPtr;

struct IJobHost;

struct IDataPipe;
typedef TIntrusivePtr<IDataPipe> IDataPipePtr;

class TErrorOutput;

DECLARE_ENUM(EJobProxyExitCode,
    ((HeartbeatFailed)       (20))
    ((ResultReportFailed)    (21))
    ((ResourcesUpdateFailed) (22))
    ((SetRLimitFailed)       (23))
    ((ExecFailed)            (24))
    ((UncaughtException)     (25))
    ((RetreiveJobSpecFailed) (26))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
