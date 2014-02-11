#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;

    TQueryAgentConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(4);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

