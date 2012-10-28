#pragma once

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/driver/config.h>

#include <ytlib/formats/format.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TFormatDefaultsConfig
    : public TYsonSerializable
{
    NFormats::TFormat Structured;
    NFormats::TFormat Tabular;

    TFormatDefaultsConfig()
    {
        // Keep this in sync with ytlib/driver/format.cpp
        auto structuredAttributes = NYTree::CreateEphemeralAttributes();
        structuredAttributes->Set("format", Stroka("pretty"));
        Register("structured", Structured)
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, ~structuredAttributes));

        auto tabularAttributes = NYTree::CreateEphemeralAttributes();
        tabularAttributes->Set("format", Stroka("text"));
        Register("tabular", Tabular)
            .Default(NFormats::TFormat(NFormats::EFormatType::Yson, ~tabularAttributes));
    }
};

typedef TIntrusivePtr<TFormatDefaultsConfig> TFormatDefaultsConfigPtr;

////////////////////////////////////////////////////////////////////////////////

struct TExecutorConfig
    : public NDriver::TDriverConfig
{
    NYTree::INodePtr Logging;
    TFormatDefaultsConfigPtr FormatDefaults;
    TDuration OperationWaitTimeout;

    TExecutorConfig()
    {
        Register("logging", Logging);
        Register("format_defaults", FormatDefaults)
            .DefaultNew();
        Register("operation_wait_timeout", OperationWaitTimeout)
            .Default(TDuration::Seconds(3));
    }
};

typedef TIntrusivePtr<TExecutorConfig> TExecutorConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
