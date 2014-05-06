#pragma once

#include <core/misc/address.h>

#include <core/ytree/yson_serializable.h>
#include <core/ytree/fluent.h>
#include <core/ytree/attribute_helpers.h>

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
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, structuredAttributes.get()));

        auto tabularAttributes = NYTree::CreateEphemeralAttributes();
        tabularAttributes->Set("format", Stroka("text"));
        RegisterParameter("tabular", Tabular)
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, tabularAttributes.get()));
    }
};

typedef TIntrusivePtr<TFormatDefaultsConfig> TFormatDefaultsConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TExecutorConfig
    : public TYsonSerializable
{
public:
    NDriver::TDriverConfigPtr Driver;
    NYTree::INodePtr Logging;
    TAddressResolverConfigPtr AddressResolver;
    TFormatDefaultsConfigPtr FormatDefaults;
    TDuration OperationPollPeriod;
    bool Trace;

    TExecutorConfig()
    {
        RegisterParameter("driver", Driver);
        RegisterParameter("logging", Logging);
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("format_defaults", FormatDefaults)
            .DefaultNew();
        RegisterParameter("operation_poll_period", OperationPollPeriod)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("trace", Trace)
            .Default(false);
    }
};

typedef TIntrusivePtr<TExecutorConfig> TExecutorConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
