#include "instance_config.h"

#include "config.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

void TSignatureValidationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_key_reader", &TThis::CypressKeyReader);

    registrar.Parameter("validator", &TThis::Validator);
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureGenerationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_key_writer", &TThis::CypressKeyWriter);

    registrar.Parameter("generator", &TThis::Generator);

    registrar.Parameter("key_rotator", &TThis::KeyRotator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
