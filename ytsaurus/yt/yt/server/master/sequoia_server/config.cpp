#include "config.h"

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicSequoiaManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("fetch_chunk_meta_from_sequoia", &TThis::FetchChunkMetaFromSequoia)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
