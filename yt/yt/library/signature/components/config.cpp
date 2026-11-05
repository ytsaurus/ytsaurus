#include "config.h"

#include <yt/yt/library/signature/generation/config.h>

#include <yt/yt/library/signature/validation/config.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSignatureComponentsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("validation", &TThis::Validation)
        .DefaultNew();

    registrar.Parameter("generation", &TThis::Generation)
        .DefaultNew();

    registrar.Parameter("use_root_user", &TThis::UseRootUser)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
