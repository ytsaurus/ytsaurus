#include "config.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;

static constexpr TYPathBuf DefaultKeyPath = "//sys/public_keys/by_owner";

////////////////////////////////////////////////////////////////////////////////

void TSignatureGeneratorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("signature_expiration_delta", &TThis::SignatureExpirationDelta)
        .Default(TDuration::Days(1))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("time_sync_margin", &TThis::TimeSyncMargin)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

void TKeyRotatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("key_rotation_options", &TThis::KeyRotationOptions)
        .Default(TRetryingPeriodicExecutorOptions(
            TPeriodicExecutorOptions{
                .Period = TDuration::Days(1),
                .Jitter = 0.1,
            },
            TExponentialBackoffOptions{
                .MinBackoff = TDuration::Seconds(1),
                .MaxBackoff = TDuration::Minutes(5),
                .BackoffMultiplier = 5.0,
            }))
        .CheckThat([] (const auto& options) {
            return options.Period && *options.Period > TDuration::MilliSeconds(100);
        });

    registrar.Parameter("key_expiration_delta", &TThis::KeyExpirationDelta)
        .Default(TDuration::Days(2))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("time_sync_margin", &TThis::TimeSyncMargin)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressKeyWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();

    registrar.Parameter("key_deletion_delay", &TThis::KeyDeletionDelay)
        .Default(TDuration::Days(1))
        .GreaterThanOrEqual(TDuration::Zero());

    registrar.Parameter("max_key_count", &TThis::MaxKeyCount)
        .Default(10)
        .GreaterThan(0);
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
