#pragma once

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/misc/address.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
    : public TYsonSerializable
{
    TAddressResolverConfigPtr AddressResolver;

    TServerConfig()
    {
        Register("address_resolver", AddressResolver)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
