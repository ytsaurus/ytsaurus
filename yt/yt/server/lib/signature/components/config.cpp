#include "config.h"

#include <yt/yt/server/lib/signature/generation/config.h>

#include <yt/yt/server/lib/signature/validation/config.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSignatureComponentsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("validation", &TThis::Validation)
        .Optional();

    registrar.Parameter("generation", &TThis::Generation)
        .Optional();

    registrar.Parameter("use_root_user", &TThis::UseRootUser)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
