#pragma once

#include "public.h"

#include <core/compression/codec.h>

#include <core/ytree/yson_serializable.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
