#include "resource.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TResourceRevision::Register(TRegistrar registrar)
{
    registrar.Parameter("revision_id", &TThis::RevisionId)
        .Default(0);
    registrar.Parameter("spec", &TThis::Spec)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
