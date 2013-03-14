#pragma once

#include "public.h"

#include <ytlib/codecs/codec.h>

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public TYsonSerializable
{
    yhash_map<Stroka, NYTree::INodePtr> Services;

    TServerConfig()
    {
        Register("services", Services)
            .Default();
    }
};

struct TServiceConfig
    : public TYsonSerializable
{
    yhash_map<Stroka, TMethodConfigPtr> Methods;

    TServiceConfig()
    {
        Register("methods", Methods)
            .Default();
    }
};

struct TMethodConfig
    : public TYsonSerializable
{
    TNullable<bool> RequestHeavy;
    TNullable<bool> ResponseHeavy;
    TNullable<ECodec> ResponseCodec;
    TNullable<int> MaxQueueSize;

    TMethodConfig()
    {
        Register("request_heavy", RequestHeavy)
            .Default();
        Register("response_heavy", ResponseHeavy)
            .Default();
        Register("response_codec", ResponseCodec)
            .Default();
        Register("max_queue_size", MaxQueueSize)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
