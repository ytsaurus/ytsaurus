#include "query.h"

namespace clickhouse {

const std::string Query::default_query_id = {};

Query::Query()
{ }

Query::Query(const char* query, const char* query_id)
    : query_(query)
    , query_id_(query_id ? std::string(query_id): default_query_id)
{
}

Query::Query(const std::string& query, const std::string& query_id)
    : query_(query)
    , query_id_(query_id)
{
}

Query::~Query()
{ }

}
