#include "config.h"

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

void TQueryAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("query_thread_pool_size", &TThis::QueryThreadPoolSize)
        .Alias("thread_pool_size")
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("lookup_thread_pool_size", &TThis::LookupThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("fetch_thread_pool_size", &TThis::FetchThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("table_row_fetch_thread_pool_size", &TThis::TableRowFetchThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("max_subsplits_per_tablet", &TThis::MaxSubsplitsPerTablet)
        .GreaterThan(0)
        .Default(4096);
    registrar.Parameter("max_subqueries", &TThis::MaxSubqueries)
        .GreaterThan(0)
        .Default(16);
    registrar.Parameter("max_query_retries", &TThis::MaxQueryRetries)
        .GreaterThanOrEqual(1)
        .Default(10);
    registrar.Parameter("desired_uncompressed_response_block_size", &TThis::DesiredUncompressedResponseBlockSize)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("function_impl_cache", &TThis::FunctionImplCache)
        .DefaultNew();

    registrar.Parameter("pool_weight_cache", &TThis::PoolWeightCache)
        .DefaultNew();

    registrar.Parameter("reject_upon_throttler_overdraft", &TThis::RejectUponThrottlerOverdraft)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->FunctionImplCache->Capacity = 100;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueryAgentDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("query_thread_pool_size", &TThis::QueryThreadPoolSize)
        .Alias("thread_pool_size")
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("lookup_thread_pool_size", &TThis::LookupThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("fetch_thread_pool_size", &TThis::FetchThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("table_row_fetch_thread_pool_size", &TThis::TableRowFetchThreadPoolSize)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("reject_upon_throttler_overdraft", &TThis::RejectUponThrottlerOverdraft)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

