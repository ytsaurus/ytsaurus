#include "config.h"

#include <yt/yt/client/api/client_common.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NYTree;
using namespace NYPath;

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
    registrar.Parameter("key_rotation_interval", &TThis::KeyRotationInterval)
        .Default(TDuration::Days(1))
        .GreaterThan(TDuration::MilliSeconds(100));

    registrar.Parameter("key_expiration_delta", &TThis::KeyExpirationDelta)
        .Default(TDuration::Days(2))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("time_sync_margin", &TThis::TimeSyncMargin)
        .Default(TDuration::Hours(1));
}

////////////////////////////////////////////////////////////////////////////////

void TCypressKeyReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();

    registrar.Parameter("cypress_read_options", &TThis::CypressReadOptions)
        .DefaultCtor([] {
            auto options = New<TSerializableMasterReadOptions>();
            options->ReadFrom = EMasterChannelKind::ClientSideCache;
            options->ExpireAfterSuccessfulUpdateTime = TDuration::Hours(12);
            return options;
        });
}

void TCypressKeyWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default(TYPath(DefaultKeyPath))
        .NonEmpty();

    registrar.Parameter("key_deletion_delay", &TThis::KeyDeletionDelay)
        .Default(TDuration::Days(1))
        .GreaterThanOrEqual(TDuration::Zero());

    // TODO(pavook) implement.
    registrar.Parameter("max_key_count", &TThis::MaxKeyCount)
        .Default(100)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureValidationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_key_reader", &TThis::CypressKeyReader);
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureGenerationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cypress_key_writer", &TThis::CypressKeyWriter);

    registrar.Parameter("generator", &TThis::Generator);

    registrar.Parameter("key_rotator", &TThis::KeyRotator);
}

////////////////////////////////////////////////////////////////////////////////

void TSignatureComponentsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("validation", &TThis::Validation)
        .Optional();

    registrar.Parameter("generation", &TThis::Generation)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
