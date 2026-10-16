#include "sink.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ISink::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void ISink::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

void ISink::PreSyncFlush()
{ }

THashMap<TStreamId, TInflightStreamTraverseDataPtr> ISink::BuildInflight() const
{
    return {};
}

void ISink::UpdateWatermarkState(TWatermarkStatePtr /*state*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
