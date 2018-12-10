#include "shell.h"
#include "private.h"

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/actions/bind.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/process.h>

#include <yt/core/net/connection.h>

#include <yt/core/pipes/pty.h>

#include <util/stream/file.h>

namespace NYT::NShell {

using namespace NConcurrency;
using namespace NCGroup;
using namespace NPipes;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

////////////////////////////////////////////////////////////////////////////////

static const char* CGroupShellPrefix = "/shell-";
static const size_t ReaderBufferSize = 4096;
static const auto PollTimeout = TDuration::Seconds(30);
static const i64 InputOffsetWarningLevel = 65536;

////////////////////////////////////////////////////////////////////////////////

class TShell
    : public IShell
{
public:
    TShell(
        std::unique_ptr<TShellOptions> options)
        : Options_(std::move(options))
        , Id_(TGuid::Create())
        , Process_(New<TSimpleProcess>(Options_->ExePath, false))
        , Freezer_(Options_->CGroupBasePath.value_or(TString()) + CGroupShellPrefix + ToString(Id_))
        , CurrentHeight_(Options_->Height)
        , CurrentWidth_(Options_->Width)
        , InactivityTimeout_(Options_->InactivityTimeout)
        , Command_(Options_->Command)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Logger.AddTag("ShellId: %v", Id_);
    }

    void Spawn()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(!IsRunning_);
        IsRunning_ = true;

        int uid = Options_->Uid.value_or(::getuid());
        auto user = SafeGetUsernameByUid(uid);
        auto home = Options_->WorkingDir;

        LOG_INFO("Spawning TTY (Term: %v, Height: %v, Width: %v, Uid: %v, Username: %v, Home: %v, InactivityTimeout: %v, Command: %v)",
            Options_->Term,
            CurrentHeight_,
            CurrentWidth_,
            uid,
            user,
            home,
            InactivityTimeout_,
            Command_);

        TPty pty(CurrentHeight_, CurrentWidth_);

        Reader_ = pty.CreateMasterAsyncReader();
        auto bufferingReader = CreateBufferingAdapter(Reader_, ReaderBufferSize);
        auto expiringReader = CreateExpiringAdapter(bufferingReader, PollTimeout);
        ConcurrentReader_ = CreateConcurrentAdapter(expiringReader);

        Writer_ = pty.CreateMasterAsyncWriter();
        ZeroCopyWriter_ = CreateZeroCopyAdapter(Writer_);

        Process_->SetWorkingDirectory(home);

        if (Options_->Command) {
            Process_->AddArguments({"--command", *Options_->Command});
        }
        Process_->AddArguments({"--pty", ::ToString(pty.GetSlaveFD())});
        Process_->AddArguments({"--uid", ::ToString(uid)});

        PrepareCGroups();

        // Init environment variables.
        Process_->AddArguments({
            "--env", "HOME=" + home,
            "--env", "LOGNAME=" + user,
            "--env", "USER=" + user,
            "--env", "TERM=" + Options_->Term,
            "--env", "LANG=en_US.UTF-8",
            "--env", "YT_SHELL_ID=" + ToString(Id_),
        });

        for (const auto& var : Options_->Environment) {
            Process_->AddArguments({"--env", var});
        }

        if (Options_->MessageOfTheDay) {
            auto path = NFS::CombinePaths(home, ".motd");

            try {
                TFile file(path, CreateAlways | WrOnly | Seq | CloseOnExec);
                TUnbufferedFileOutput output(file);
                output.Write(Options_->MessageOfTheDay->c_str(), Options_->MessageOfTheDay->size());
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error saving shell message file")
                    << ex
                    << TErrorAttribute("path", path);
            }
        }
        if (Options_->Bashrc) {
            auto path = NFS::CombinePaths(home, ".bashrc");

            try {
                TFile file(path, CreateAlways | WrOnly | Seq | CloseOnExec);
                TUnbufferedFileOutput output(file);
                output.Write(Options_->Bashrc->c_str(), Options_->Bashrc->size());
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error saving shell config file")
                    << ex
                    << TErrorAttribute("path", path);
            }
        }
        ResizeWindow(CurrentHeight_, CurrentWidth_);
        Process_->Spawn()
            .Subscribe(
                BIND(&TShell::Terminate, MakeWeak(this))
                    .Via(GetCurrentInvoker()));
        LOG_INFO("Shell started");
    }

    virtual void ResizeWindow(int height, int width) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (CurrentHeight_ != height || CurrentWidth_ != width) {
            SafeSetTtyWindowSize(Reader_->GetHandle(), height, width);
            CurrentHeight_ = height;
            CurrentWidth_ = width;
        }
    }

    virtual ui64 SendKeys(const TSharedRef& keys, ui64 inputOffset) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ConsumedOffset_ < inputOffset) {
            // Key sequence from the future is not possible.
            THROW_ERROR_EXCEPTION("Input offset is more than consumed offset")
                << TErrorAttribute("expected_input_offset", ConsumedOffset_)
                << TErrorAttribute("actual_input_offset", inputOffset);
        }

        if (inputOffset + InputOffsetWarningLevel < ConsumedOffset_) {
            LOG_WARNING(
                "Input offset is significantly less than consumed offset (InputOffset: %v, ConsumedOffset: %v)",
                ConsumedOffset_,
                inputOffset);
        }

        size_t offset = ConsumedOffset_ - inputOffset;
        if (offset < keys.Size()) {
            ConsumedOffset_ += keys.Size() - offset;
            ZeroCopyWriter_->Write(keys.Slice(offset, keys.Size()));

            LastActivity_ = TInstant::Now();
            if (InactivityCookie_) {
                TDelayedExecutor::CancelAndClear(InactivityCookie_);
                InactivityCookie_ = TDelayedExecutor::Submit(
                    BIND(&TShell::Terminate, MakeWeak(this), InactivityError_)
                        .Via(GetCurrentInvoker()),
                    InactivityTimeout_);
            }
        }
        return ConsumedOffset_;
    }

    virtual TFuture<TSharedRef> Poll() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return ConcurrentReader_->Read();
    }

    virtual void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!IsRunning_) {
            return;
        }
        IsRunning_ = false;

        TDelayedExecutor::CancelAndClear(InactivityCookie_);
        Writer_->Abort();
        CleanupShellProcesses();
        TerminatedPromise_.TrySet();
        LOG_INFO(error, "Shell terminated");
    }

    virtual TFuture<void> Shutdown(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsRunning_ && !InactivityCookie_) {
            auto delay = InactivityTimeout_;
            InactivityError_ = error;
            InactivityCookie_ = TDelayedExecutor::Submit(
                BIND(&TShell::Terminate, MakeWeak(this), InactivityError_)
                    .Via(GetCurrentInvoker()),
                delay);
        }
        return TerminatedPromise_;
    }

    virtual const TShellId& GetId() override
    {
        return Id_;
    }

private:
    const std::unique_ptr<TShellOptions> Options_;
    const TShellId Id_;
    const TProcessBasePtr Process_;
    TFreezer Freezer_;
    int CurrentHeight_;
    int CurrentWidth_;

    bool IsRunning_ = false;

    IConnectionWriterPtr Writer_;
    IAsyncZeroCopyOutputStreamPtr ZeroCopyWriter_;
    ui64 ConsumedOffset_ = 0;

    IConnectionReaderPtr Reader_;
    IAsyncZeroCopyInputStreamPtr ConcurrentReader_;

    TInstant LastActivity_ = TInstant::Now();
    TDuration InactivityTimeout_;
    TDelayedExecutorCookie InactivityCookie_;
    TError InactivityError_;
    TPromise<void> TerminatedPromise_ = NewPromise<void>();

    std::optional<TString> Command_;

    NLogging::TLogger Logger = ShellLogger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void PrepareCGroups()
    {
        if (!Options_->CGroupBasePath) {
            return;
        }

        try {
            Freezer_.Create();
            Process_->AddArguments({ "--cgroup", Freezer_.GetFullPath() });
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create required cgroups");
        }
    }

    void CleanupShellProcesses()
    {
        if (!Options_->CGroupBasePath) {
            // We can not kill shell without cgroup (shell has different euid),
            // so abort reader
            Reader_->Abort();
            return;
        }

        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may still be alive.
            RunKiller(Freezer_.GetFullPath());
            Freezer_.Destroy();
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to clean up shell processes");
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

IShellPtr CreateShell(std::unique_ptr<TShellOptions> options)
{
    auto shell = New<TShell>(std::move(options));
    shell->Spawn();
    return shell;
}

#else

IShellPtr CreateShell(std::unique_ptr<TShellOptions> /*options*/)
{
    THROW_ERROR_EXCEPTION("Shell is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
