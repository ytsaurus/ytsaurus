#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionControllerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration DataNodePingPeriod;
    TDuration WriteSessionPingPeriod;
    std::string Account;
    int ReplicationFactor;

    TDuration NodeRpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedChunkSessionControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkSessionControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkWriterConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
