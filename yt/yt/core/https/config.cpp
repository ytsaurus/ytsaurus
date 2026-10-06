#include "config.h"

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

void TServerCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cert_sensors_update_period", &TThis::CertSensorsUpdatePeriod)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("credentials", &TThis::Credentials);

    registrar.Preprocessor([] (TThis* config) {
        config->ServerName = "Https";
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClientCredentialsConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("credentials", &TThis::Credentials)
        .Optional();
    registrar.Parameter("allow_http", &TThis::AllowHttp)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
