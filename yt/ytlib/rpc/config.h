#pragma once

#include "public.h"

#include <ytlib/compression/codec.h>

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TYsonSerializable
{
public:
    yhash_map<Stroka, NYTree::INodePtr> Services;

    TServerConfig()
    {
        RegisterParameter("services", Services)
            .Default();
    }
};

class TServiceConfig
    : public TYsonSerializable
{
public:
    yhash_map<Stroka, TMethodConfigPtr> Methods;

    TServiceConfig()
    {
        RegisterParameter("methods", Methods)
            .Default();
    }
};

class TMethodConfig
    : public TYsonSerializable
{
public:
    TNullable<bool> RequestHeavy;
    TNullable<bool> ResponseHeavy;
    TNullable<NCompression::ECodec> ResponseCodec;
    TNullable<int> MaxQueueSize;

    TMethodConfig()
    {
        RegisterParameter("request_heavy", RequestHeavy)
            .Default();
        RegisterParameter("response_heavy", ResponseHeavy)
            .Default();
        RegisterParameter("response_codec", ResponseCodec)
            .Default();
        RegisterParameter("max_queue_size", MaxQueueSize)
            .Default();
    }
};

class TThrottlingChannelConfig
    : public TYsonSerializable
{
public:
    //! Maximum allowed number of requests per second.
    int RateLimit;

    TThrottlingChannelConfig()
    {
        RegisterParameter("rate_limit", RateLimit)
            .GreaterThan(0)
            .Default(10);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
