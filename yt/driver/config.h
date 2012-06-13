#pragma once

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/driver/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TFormatDefaultsConfig
    : public TYsonSerializable
{
    NYTree::INodePtr Structured;
    NYTree::INodePtr Tabular;

    TFormatDefaultsConfig()
    {
        // Keep this in sync with ytlib/driver/format.cpp
        Register("structured", Structured)
            .Default(NYTree::ConvertToNode(
                NYTree::BuildYsonFluently()
                    .BeginAttributes()
                        .Item("format").Scalar("pretty")
                    .EndAttributes()
                    .Scalar("yson").GetYsonString()));
        Register("tabular", Tabular)
            .Default(NYTree::ConvertToNode(NYTree::BuildYsonFluently()
                .BeginAttributes()
                    .Item("format").Scalar("text")
                .EndAttributes()
                .Scalar("yson").GetYsonString()));
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
