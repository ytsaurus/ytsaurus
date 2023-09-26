#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellParameters
    : public NYTree::TYsonStructLite
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

    REGISTER_YSON_STRUCT_LITE(TShellParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TShellResult
    : public NYTree::TYsonStructLite
{
    // TODO(gritukan): Deprecate ShellId someday.
    TShellId ShellId;
    int ShellIndex;
    std::optional<TString> Output;
    std::optional<ui64> ConsumedOffset;

    REGISTER_YSON_STRUCT_LITE(TShellResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
