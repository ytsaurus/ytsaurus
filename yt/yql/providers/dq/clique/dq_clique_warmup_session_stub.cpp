#include "dq_clique_warmup_session.h"

namespace NYql {

IDqCliqueWarmupSessionPtr CreateDqCliqueWarmupSession(
    TDqWarmupIsReadyFn /*isReadyFn*/,
    TVector<TResourceManagerOptions> /*ytBackends*/)
{
    return nullptr;
}

} // namespace NYql
