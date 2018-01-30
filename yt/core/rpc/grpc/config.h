#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {
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
    yhash<TString, NYTree::INodePtr> GrpcArguments;

    TServerConfig()
    {
        RegisterParameter("addresses", Addresses);
        RegisterParameter("grpc_arguments", GrpcArguments)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

class TChannelConfig
    : public NYTree::TYsonSerializable
{
public:
    EAddressType Type;
    TString Address;
    yhash<TString, NYTree::INodePtr> GrpcArguments;

    TChannelConfig()
    {
        RegisterParameter("type", Type)
            .Default(EAddressType::Insecure);
        RegisterParameter("address", Address);
        RegisterParameter("grpc_arguments", GrpcArguments)
            .Default();
    }

    static TChannelConfigPtr CreateInsecure(const TString& address)
    {
        auto config = New<TChannelConfig>();
        config->Address = address;
        return config;
    }
};

DEFINE_REFCOUNTED_TYPE(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT
