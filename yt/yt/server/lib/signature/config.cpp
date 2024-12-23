#include "config.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TSignatureGeneratorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("key_expiration_delta", &TThis::KeyExpirationDelta)
        .Default(TDuration::Days(2))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("signature_expiration_delta", &TThis::SignatureExpirationDelta)
        .Default(TDuration::Days(1))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("time_sync_margin", &TThis::TimeSyncMargin)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureValidatorConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TKeyRotatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("key_rotation_interval", &TThis::KeyRotationInterval)
        .Default(TDuration::Days(1))
        .GreaterThan(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
