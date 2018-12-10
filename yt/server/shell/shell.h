#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/ref.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

struct TShellOptions
{
    TString ExePath = "/bin/bash";
    std::optional<int> Uid;
    TString Term = "xterm";
    int Height = 24;
    int Width = 80;
    TString WorkingDir = "/";
    std::optional<TString> CGroupBasePath;
    std::vector<TString> Environment;
    std::optional<TString> Bashrc;
    std::optional<TString> MessageOfTheDay;
    TDuration InactivityTimeout;
    std::optional<TString> Command;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a shell running inside job sandbox.
struct IShell
    : public virtual TRefCounted
{
    virtual const TShellId& GetId() = 0;
    virtual void ResizeWindow(int height, int width) = 0;
    //! Inserts keys into input sequence at specified offset.
    //! Returns consumed offset of the input sequence.
    //! This function is NOT thread-safe.
    virtual ui64 SendKeys(const TSharedRef& keys, ui64 inputOffset) = 0;
    virtual TFuture<TSharedRef> Poll() = 0;
    //! Tries to clean up, best effort guarantees.
    virtual void Terminate(const TError& error) = 0;
    //! Asynchronously wait for inactivity timeout and terminate.
    virtual TFuture<void> Shutdown(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IShell)

////////////////////////////////////////////////////////////////////////////////

IShellPtr CreateShell(std::unique_ptr<TShellOptions> options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
