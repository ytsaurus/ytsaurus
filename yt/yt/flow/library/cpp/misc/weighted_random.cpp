#include "weighted_random.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

double IRandomDoubleProvider::Get(double min, double max)
{
    return min + (max - min) * Get();
}

double TDefaultRandomDoubleProvider::Get()
{
    return RandomNumber<double>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
