#include "signaler.h"

#include "registry.h"

#include <yt/yt/core/misc/proc.h>

namespace NYT::NTools {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// NB: Moving this into the header will break tool registration as the linker
// will optimize it out.
void TSignalerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pids", &TThis::Pids)
        .Default();
    registrar.Parameter("signal_name", &TThis::SignalName)
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
