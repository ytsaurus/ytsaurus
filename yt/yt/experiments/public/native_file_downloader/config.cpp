#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("speed_measurement_window", &TConfig::SpeedMesaurementWindow)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("reader", &TConfig::Reader)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT