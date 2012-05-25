#pragma once

#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/driver/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TFormatDefaultsConfig
    : public TConfigurable
{
    NYTree::INodePtr Structured;
    NYTree::INodePtr Tabular;

    TFormatDefaultsConfig()
    {
        // Keep this in sync with ytlib/driver/format.cpp
        Register("structured", Structured)
            .Default(NYTree::DeserializeFromYson(NYTree::BuildYsonFluently()
                .BeginAttributes()
                    .Item("format").Scalar("pretty")
                .EndAttributes()
                .Scalar("yson")));
        Register("tabular", Tabular)
            .Default(NYTree::DeserializeFromYson(NYTree::BuildYsonFluently()
                .BeginAttributes()
                    .Item("format").Scalar("text")
                .EndAttributes()
                .Scalar("yson")));
    }
};

typedef TIntrusivePtr<TFormatDefaultsConfig> TDefaultFormatConfigPtr;

////////////////////////////////////////////////////////////////////////////////

struct TExecutorConfig
    : public NDriver::TDriverConfig
{
    NYTree::INodePtr Logging;
    TDefaultFormatConfigPtr FormatDefaults;
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

} // namespace NYT
