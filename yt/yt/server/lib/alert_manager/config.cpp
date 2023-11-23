#include "config.h"

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

void TAlertManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alert_collection_period", &TThis::AlertCollectionPeriod)
        .Default(TDuration::MilliSeconds(500));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
