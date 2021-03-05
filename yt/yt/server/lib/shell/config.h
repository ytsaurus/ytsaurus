#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellParameters
    : public NYTree::TYsonSerializableLite
{
    // TODO(gritukan): Deprecate ShellId someday;
    std::optional<TShellId> ShellId;
    std::optional<int> ShellIndex;
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
        RegisterParameter("shell_index", ShellIndex)
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
    // TODO(gritukan): Deprecate ShellId someday.
    TShellId ShellId;
    int ShellIndex;
    std::optional<TString> Output;
    std::optional<ui64> ConsumedOffset;

    TShellResult()
    {
        RegisterParameter("shell_id", ShellId);
        RegisterParameter("shell_index", ShellIndex);
        RegisterParameter("output", Output);
        RegisterParameter("consumed_offset", ConsumedOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
