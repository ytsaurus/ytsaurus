#pragma once

#include <ytlib/misc/address.h>

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/driver/config.h>

#include <ytlib/formats/format.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TFormatDefaultsConfig
    : public TYsonSerializable
{
public:
    NFormats::TFormat Structured;
    NFormats::TFormat Tabular;

    TFormatDefaultsConfig()
    {
        // Keep this in sync with ytlib/driver/format.cpp
        auto structuredAttributes = NYTree::CreateEphemeralAttributes();
        structuredAttributes->Set("format", Stroka("pretty"));
        RegisterParameter("structured", Structured)
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, ~structuredAttributes));

        auto tabularAttributes = NYTree::CreateEphemeralAttributes();
        tabularAttributes->Set("format", Stroka("text"));
        RegisterParameter("tabular", Tabular)
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, ~tabularAttributes));
    }
};

typedef TIntrusivePtr<TFormatDefaultsConfig> TFormatDefaultsConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TExecutorConfig
    : public NDriver::TDriverConfig
{
public:
    NYTree::INodePtr Logging;
    TAddressResolverConfigPtr AddressResolver;
    TFormatDefaultsConfigPtr FormatDefaults;
    TDuration OperationWaitTimeout;

    TExecutorConfig()
    {
        RegisterParameter("logging", Logging);
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("format_defaults", FormatDefaults)
            .DefaultNew();
        RegisterParameter("operation_wait_timeout", OperationWaitTimeout)
            .Default(TDuration::Seconds(3));
    }
};

typedef TIntrusivePtr<TExecutorConfig> TExecutorConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
