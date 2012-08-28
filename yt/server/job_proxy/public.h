#pragma once

#include <ytlib/scheduler/public.h>
#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TJobId;

struct TJobIOConfig;
typedef TIntrusivePtr<TJobIOConfig> TJobIOConfigPtr;

struct TJobProxyConfig;
typedef TIntrusivePtr<TJobProxyConfig> TJobProxyConfigPtr;

class TUserJobIO;

struct IJob;
typedef TIntrusivePtr<IJob> TJobPtr;

struct IJobHost;

struct IDataPipe;
typedef TIntrusivePtr<IDataPipe> TDataPipePtr;

class TErrorOutput;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
