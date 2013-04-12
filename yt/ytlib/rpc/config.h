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
        Register("services", Services)
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
        Register("methods", Methods)
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
