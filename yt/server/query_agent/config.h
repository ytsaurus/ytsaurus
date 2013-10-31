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
    int PoolSize;

    TQueryAgentConfig()
    {
        RegisterParameter("pool_size", PoolSize)
            .Default(4);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

