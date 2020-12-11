#include "config.h"

namespace NYT::NDynamicConfig {

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManagerConfig::TDynamicConfigManagerConfig()
{
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("enable_unrecognized_options_alert", EnableUnrecognizedOptionsAlert)
        .Default(false);
    RegisterParameter("ignore_config_absence", IgnoreConfigAbsence)
        .Default(false);
}

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
