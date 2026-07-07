#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TMinHashSimilarityConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_similarity", &TThis::MinSimilarity)
        .InRange(0, 1)
        .Default(0.5);
    registrar.Parameter("min_row_count", &TThis::MinRowCount)
        .GreaterThan(0)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
