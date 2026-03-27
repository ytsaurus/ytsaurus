#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedChunkSessionControllerConfig
    : public NYTree::TYsonStruct
{
    TDuration SessionPingPeriod;
    TDuration SessionTimeout;
    std::string Account;
    std::string MediumName;
    bool IsVital;

    TDuration NodeRpcTimeout;
    TDuration CreateChunkTimeout;

    // Number of consecutive ping failures after which the controller
    // considers the session lost and closes with an error.
    int MaxConsecutivePingFailures;

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
