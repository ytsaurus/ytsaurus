#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TMisraGriesHeavyHittersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("window", &TThis::Window)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("default_limit", &TThis::DefaultLimit)
        .GreaterThan(0)
        .Default(50);
    registrar.Parameter("threshold", &TThis::Threshold)
        .InRange(1e-5, 1)
        .Default(0.001);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
