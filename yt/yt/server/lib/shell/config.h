#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellParameters
    : public NYTree::TYsonSerializableLite
{
    TShellId ShellId;
    EShellOperation Operation;
    std::optional<TString> Term;
    TString Keys;
    std::optional<ui64> InputOffset;
    int Height;
    int Width;
    //! Timeout for inactive shell after failed or completed job.
    TDuration InactivityTimeout;
    //! Environment variables passed to job shell.
    std::vector<TString> Environment;
    std::optional<TString> Command;

    TShellParameters()
    {
        RegisterParameter("shell_id", ShellId)
            .Default();
        RegisterParameter("operation", Operation);
        RegisterParameter("term", Term)
            .Default();
        RegisterParameter("keys", Keys)
            .Default();
        RegisterParameter("input_offset", InputOffset)
            .Default();
        RegisterParameter("height", Height)
            .Default(0);
        RegisterParameter("width", Width)
            .Default(0);
        RegisterParameter("inactivity_timeout", InactivityTimeout)
            .Default(TDuration::Seconds(5 * 60));
        RegisterParameter("environment", Environment)
            .Default();
        RegisterParameter("command", Command)
            .Default();

        RegisterPostprocessor([&] () {
            if (Operation != EShellOperation::Spawn && !ShellId) {
                THROW_ERROR_EXCEPTION(
                    "Malformed request: shell id is not specified for %Qlv operation",
                    Operation);
            }
            if (Operation == EShellOperation::Update && !Keys.empty() && !InputOffset) {
                THROW_ERROR_EXCEPTION(
                    "Malformed request: input offset is not specified for %Qlv operation",
                    Operation);
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TShellResult
    : public NYTree::TYsonSerializableLite
{
    TShellId ShellId;
    std::optional<TString> Output;
    std::optional<ui64> ConsumedOffset;

    TShellResult()
    {
        RegisterParameter("shell_id", ShellId);
        RegisterParameter("output", Output);
        RegisterParameter("consumed_offset", ConsumedOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
