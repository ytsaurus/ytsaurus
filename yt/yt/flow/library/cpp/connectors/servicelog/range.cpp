#include "range.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TServiceLogEndpoint::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("exclusive", &TThis::Exclusive)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TServiceLogRange::Register(TRegistrar registrar)
{
    registrar.Parameter("lower", &TThis::Lower)
        .Default();
    registrar.Parameter("upper", &TThis::Upper)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
