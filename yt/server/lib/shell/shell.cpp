#include "shell.h"
#include "private.h"

#include <yt/server/lib/containers/instance.h>

#include <yt/server/lib/misc/process.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/library/process/process.h>
#include <yt/library/process/pty.h>

#include <yt/core/actions/bind.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/net/connection.h>

#include <util/stream/file.h>

namespace NYT::NShell {

using namespace NConcurrency;
using namespace NContainers;
using namespace NCGroup;
using namespace NPipes;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

////////////////////////////////////////////////////////////////////////////////

static const size_t ReaderBufferSize = 4096;
static constexpr auto TerminatedShellReadTimeout = TDuration::Seconds(2);
static constexpr auto PollTimeout = TDuration::Seconds(30);
static const i64 InputOffsetWarningLevel = 65536;

////////////////////////////////////////////////////////////////////////////////

class TShell
    : public IShell
{
public:
    TShell(
        IPortoExecutorPtr portoExecutor,
        std::unique_ptr<TShellOptions> options)
        : PortoExecutor_(std::move(portoExecutor))
        , Options_(std::move(options))
        , Id_(Options_->Id)
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

        YT_VERIFY(!IsRunning_);
        IsRunning_ = true;

        YT_LOG_INFO("Spawning job shell container (ContainerName: %v)", Options_->ContainerName);
        Instance_ = CreatePortoInstance(Options_->ContainerName, PortoExecutor_);

        int uid = Options_->Uid.value_or(::getuid());
        auto user = SafeGetUsernameByUid(uid);
        auto home = Options_->WorkingDir;

        YT_LOG_INFO("Spawning TTY (Term: %v, Height: %v, Width: %v, Uid: %v, Username: %v, Home: %v, InactivityTimeout: %v, Command: %v)",
            Options_->Term,
            CurrentHeight_,
            CurrentWidth_,
            uid,
            user,
            home,
            InactivityTimeout_,
            Command_);

        Pty_ = std::make_unique<TPty>(CurrentHeight_, CurrentWidth_);
        const TString tty = Format("/dev/fd/%v", Pty_->GetSlaveFD());

        Instance_->SetStdIn(tty);
        Instance_->SetStdOut(tty);
        Instance_->SetStdErr(tty);

        // NB(gritukan, psushin): Porto is unable to resolve username inside subcontainer
        // so we pass uid instead.
        Instance_->SetUser(ToString(uid));

        Process_ = New<TPortoProcess>("/bin/bash", Instance_, false);
        if (Options_->Command) {
            Process_->AddArguments({"-c", *Options_->Command});
        }

        Reader_ = Pty_->CreateMasterAsyncReader();
        auto bufferingReader = CreateBufferingAdapter(Reader_, ReaderBufferSize);
        ConcurrentReader_ = CreateConcurrentAdapter(bufferingReader);

        Writer_ = Pty_->CreateMasterAsyncWriter();
        ZeroCopyWriter_ = CreateZeroCopyAdapter(Writer_);

        Process_->SetWorkingDirectory(home);

        auto addEnv = [&] (const TString& name, const TString& value) {
            Process_->AddEnvVar(Format("%v=%v", name, value));
        };

        // Init environment variables.
        addEnv("HOME", home);
        addEnv("LOGNAME", user);
        addEnv("USER", user);
        addEnv("TERM", Options_->Term);
        addEnv("LANG", "en_US.UTF-8");
        addEnv("YT_SHELL_ID", ToString(Id_));

        for (const auto& var : Options_->Environment) {
            Process_->AddEnvVar(var);
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
        YT_LOG_INFO("Shell started");
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
            YT_LOG_WARNING(
                "Input offset is significantly less than consumed offset (InputOffset: %v, ConsumedOffset: %v)",
                ConsumedOffset_,
                inputOffset);
        }

        size_t offset = ConsumedOffset_ - inputOffset;
        if (offset < keys.Size()) {
            ConsumedOffset_ += keys.Size() - offset;
            WaitFor(ZeroCopyWriter_->Write(keys.Slice(offset, keys.Size())))
                .ThrowOnError();

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

        return ConcurrentReader_->Read()
            .WithTimeout(PollTimeout);
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
        Instance_->Destroy();
        Reader_->SetReadDeadline(TInstant::Now() + TerminatedShellReadTimeout);
        TerminatedPromise_.TrySet();
        YT_LOG_INFO(error, "Shell terminated");
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

    virtual TShellId GetId() override
    {
        return Id_;
    }

    virtual bool Terminated() const override
    {
        return TerminatedPromise_.IsSet();
    }

private:
    const IPortoExecutorPtr PortoExecutor_;
    const std::unique_ptr<TShellOptions> Options_;
    const TShellId Id_;
    int CurrentHeight_;
    int CurrentWidth_;

    IInstancePtr Instance_;
    TProcessBasePtr Process_;

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

    std::unique_ptr<TPty> Pty_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IShellPtr CreateShell(
    NContainers::IPortoExecutorPtr portoExecutor,
    std::unique_ptr<TShellOptions> options)
{
    auto shell = New<TShell>(std::move(portoExecutor), std::move(options));
    shell->Spawn();
    return shell;
}

#else

IShellPtr CreateShell(
    NContainers::IPortoExecutorPtr /*portoExecutor*/,
    std::unique_ptr<TShellOptions> /*options*/)
{
    THROW_ERROR_EXCEPTION("Shell is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
