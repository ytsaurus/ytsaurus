#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAddressType,
    (Insecure)
);

class TServerAddressConfig
    : public NYTree::TYsonSerializable
{
public:
    EAddressType Type;
    TString Address;

    TServerAddressConfig()
    {
        RegisterParameter("type", Type)
            .Default(EAddressType::Insecure);
        RegisterParameter("address", Address);
    }
};

DEFINE_REFCOUNTED_TYPE(TServerAddressConfig)

class TServerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TServerAddressConfigPtr> Addresses;

    TServerConfig()
    {
        RegisterParameter("addresses", Addresses);
    }
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

class TChannelConfig
    : public NYTree::TYsonSerializable
{
public:
    EAddressType Type;
    TString Address;

    TChannelConfig()
    {
        RegisterParameter("type", Type)
            .Default(EAddressType::Insecure);
        RegisterParameter("address", Address);
    }
};

DEFINE_REFCOUNTED_TYPE(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NYT
