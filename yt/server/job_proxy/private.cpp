#include "private.h"

//#include <yt/ytlib/job_proxy/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger JobProxyLogger("JobProxy");
const NProfiling::TProfiler JobProxyProfiler("/job_proxy");
const TDuration RpcServerShutdownTimeout = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

