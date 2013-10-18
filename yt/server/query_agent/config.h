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
    int WorkerPoolSize;

    TQueryAgentConfig()
    {
        RegisterParameter("worker_pool_size", WorkerPoolSize)
            .Default(4);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

