#include "yql_ytflow_monium_clients_cache.h"


namespace NYql::NYtflow {

DEFINE_REFCOUNTED_TYPE(IMoniumClientsCache);

IMoniumClientsCachePtr CreateMoniumClientsCache()
{
    return {};
}

} // namespace NYql::NYtflow
