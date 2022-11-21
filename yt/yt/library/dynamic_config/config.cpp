#include "config.h"

namespace NYT::NDynamicConfig {

////////////////////////////////////////////////////////////////////////////////

void TDynamicConfigManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("enable_unrecognized_options_alert", &TThis::EnableUnrecognizedOptionsAlert)
        .Default(false);
    registrar.Parameter("ignore_config_absence", &TThis::IgnoreConfigAbsence)
        .Default(false);
}

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
