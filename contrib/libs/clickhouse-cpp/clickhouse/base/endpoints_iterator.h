#pragma once

#include "clickhouse/client.h"
#include <vector>

namespace clickhouse {

struct ClientOptions;

/**
 * Base class for iterating through endpoints.
*/
class EndpointsIteratorBase
{
 public:
   virtual ~EndpointsIteratorBase() = default;

   virtual Endpoint Next() = 0;
};

class RoundRobinEndpointsIterator : public EndpointsIteratorBase
{
 public:
    explicit RoundRobinEndpointsIterator(const std::vector<Endpoint>& opts);
    Endpoint Next() override;

    ~RoundRobinEndpointsIterator() override;

 private:
    const std::vector<Endpoint>& endpoints;
    size_t current_index;
};

}
