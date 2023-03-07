#include "signaler.h"

#include <yt/core/misc/proc.h>

#include <yt/ytlib/tools/registry.h>

namespace NYT::NTools {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// NB: Moving this into the header will break tool registration as the linker
// will optimize it out.
TSignalerConfig::TSignalerConfig()
{
    RegisterParameter("pids", Pids)
        .Default();
    RegisterParameter("signal_name", SignalName)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TSignalerTool::operator()(const TSignalerConfigPtr& arg) const
{
    TrySetUid(0);
    SendSignal(arg->Pids, arg->SignalName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
