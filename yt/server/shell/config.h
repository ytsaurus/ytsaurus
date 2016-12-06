#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellParameters
    : public NYTree::TYsonSerializableLite
{
    TShellId ShellId;
    EShellOperation Operation;
    TNullable<Stroka> Term;
    Stroka Keys;
    TNullable<ui64> InputOffset;
    int Height;
    int Width;
    //! Timeout for inactive shell after failed or completed job.
    TDuration InactivityTimeout;
    //! Environment variables passed to job shell.
    std::vector<Stroka> Environment;

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

        RegisterValidator([&] () {
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
    TNullable<Stroka> Output;
    TNullable<ui64> ConsumedOffset;

    TShellResult()
    {
        RegisterParameter("shell_id", ShellId);
        RegisterParameter("output", Output);
        RegisterParameter("consumed_offset", ConsumedOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NShell
} // namespace NYT
