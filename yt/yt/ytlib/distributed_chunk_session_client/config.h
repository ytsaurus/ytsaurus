#pragma once

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <limits>

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

struct TDistributedChunkSessionPoolConfig
    : public NYTree::TYsonStruct
{
    int MaxActiveSessionsPerSlot;
    TExponentialBackoffOptions ChunkSealRetryBackoff;
    TDuration ChunkSealRpcTimeout;

    REGISTER_YSON_STRUCT(TDistributedChunkSessionPoolConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDistributedChunkSessionPoolConfig)

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
