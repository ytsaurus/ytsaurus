#include "authentication.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TControllerRequestMetadata::Register(TRegistrar registrar)
{
    registrar.Parameter("method", &TThis::Method);
    registrar.Parameter("pipeline_object_id", &TThis::PipelineObjectId);
    registrar.Parameter("controller_address", &TThis::ControllerAddress);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
