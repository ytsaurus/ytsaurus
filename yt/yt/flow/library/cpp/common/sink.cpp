#include "sink.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ISink::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void ISink::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

void ISink::Terminate()
{ }

void ISink::PreSyncFlush()
{ }

THashMap<TStreamId, TInflightStreamTraverseDataPtr> ISink::BuildInflight() const
{
    return {};
}

void ISink::UpdateWatermarkState(TWatermarkStatePtr /*state*/)
{ }

NYTree::IMapNodePtr ISink::GetPartitionStatus() const
{
    return nullptr;
}

void ISink::BeforeGracefulShutdown()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
