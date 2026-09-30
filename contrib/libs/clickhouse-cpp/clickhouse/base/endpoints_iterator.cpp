#include "endpoints_iterator.h"
#include <clickhouse/client.h>

namespace clickhouse {

RoundRobinEndpointsIterator::RoundRobinEndpointsIterator(const std::vector<Endpoint>& _endpoints)
   :  endpoints (_endpoints)
   , current_index (endpoints.size() - 1ull)
{
}

Endpoint RoundRobinEndpointsIterator::Next()
{
   current_index = (current_index + 1ull) % endpoints.size();
   return endpoints[current_index];
}

RoundRobinEndpointsIterator::~RoundRobinEndpointsIterator() = default;

}
