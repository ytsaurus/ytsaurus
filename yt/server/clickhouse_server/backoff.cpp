#include "backoff.h"

#include <util/random/random.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TDuration AddJitter(const TDuration& d, double jitter)
{
    double correction = 1 - jitter + RandomNumber<double>() * (2 * jitter);
    return d * correction;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
