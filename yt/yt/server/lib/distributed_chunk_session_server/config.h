#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionServiceConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration SessionTimeout;
    TDuration DataNodeRpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedChunkSessionServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkSessionServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
