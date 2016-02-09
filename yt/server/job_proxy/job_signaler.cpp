#include "job_signaler.h"
#include "private.h"

#include <yt/ytlib/job_prober_client/job_signal.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/subprocess.h>

#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/serialize.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NConcurrency;
using namespace NJobProberClient;

////////////////////////////////////////////////////////////////////////////////

void TJobSignalerTool::operator()(const TJobSignalerArg& arg) const
{
    SafeSetUid(0);
    return SendSignal(arg.Pids, arg.SignalName);
}

REGISTER_TOOL(TJobSignalerTool);

////////////////////////////////////////////////////////////////////////////////

void SendSignal(const std::vector<int>& pids, const Stroka& signalName)
{
    ValidateSignalName(signalName);

    auto sig = FindSignalIdBySignalName(signalName);
    for (int pid : pids) {
        kill(pid, *sig);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
