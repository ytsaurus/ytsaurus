#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedChunkSessionControllerConfig
    : public NYTree::TYsonStruct
{
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

struct TDistributedChunkWriterConfig
    : public NYTree::TYsonStruct
{
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
