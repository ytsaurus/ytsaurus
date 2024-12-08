#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TConfig::Reader)
        .DefaultNew();
    registrar.Parameter("multiplexing_parallelism", &TConfig::MultiplexingParallelism)
        .Default(1);
    registrar.Parameter("block_read_parallelism", &TConfig::BlockReadParallelism)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT