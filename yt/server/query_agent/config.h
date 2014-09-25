#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/query_client/config.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentConfig
    : public NQueryClient::TExecutorConfig
{
public:
    int ThreadPoolSize;
    int MaxConcurrentRequests;
    int MaxSubsplitsPerTablet;
    int MaxQueryRetries;

    TQueryAgentConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("max_concurrent_requests", MaxConcurrentRequests)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("max_subsplits_per_tablet", MaxSubsplitsPerTablet)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("max_query_retries", MaxQueryRetries)
            .GreaterThanOrEqual(1)
            .Default(10);
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

