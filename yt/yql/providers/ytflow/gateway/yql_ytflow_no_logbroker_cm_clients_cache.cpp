#include "yql_ytflow_logbroker_cm_clients_cache.h"


namespace NYql::NYtflow {

DEFINE_REFCOUNTED_TYPE(ILogbrokerCmClientsCache);

ILogbrokerCmClientsCachePtr CreateLogbrokerCmClientsCache()
{
    return {};
}

} // namespace NYql::NYtflow
