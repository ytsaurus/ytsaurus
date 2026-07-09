#include "companion_model.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TStreamSpecsPtr TCompanionProcessRequest::GetMessageStreamSpecs() const
{
    return OverrideStreamSpecs ? OverrideStreamSpecs : JobStreamSpecs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
