#include "shell.h"
#include "private.h"

#ifdef _linux_

#include <yt/yt/library/containers/instance.h>

#endif

#include <yt/yt/server/lib/user_job/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/public.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/library/pipe_io/pty.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

#include <util/stream/file.h>

namespace NYT::NShell {

using namespace NConcurrency;
using namespace NContainers;
using namespace NPipeIO;
using namespace NNet;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

////////////////////////////////////////////////////////////////////////////////

static const size_t ReaderBufferSize = 4096;
static constexpr auto TerminatedShellReadTimeout = TDuration::Seconds(2);
static constexpr auto PollTimeout = TDuration::Seconds(30);
static const i64 InputOffsetWarningLevel = 65536;

////////////////////////////////////////////////////////////////////////////////


class TShellBase
    : public IShell
{
public:
    TShellBase(
        std::unique_ptr<TShellOptions> options)
        : Options_(std::move(options))
        , Id_(Options_->Id)
        , Index_(Options_->Index)
        , CurrentHeight_(Options_->Height)
        , CurrentWidth_(Options_->Width)
        , InactivityTimeout_(Options_->InactivityTimeout)
        , Command_(Options_->Command)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        Logger.AddTag("ShellId: %v, ShellIndex: %v", Id_, Index_);
    }

    void ResizeWindow(int height, int width) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (CurrentHeight_ != height || CurrentWidth_ != width) {
            SafeSetTtyWindowSize(Reader_->GetHandle(), height, width);
            CurrentHeight_ = height;
            CurrentWidth_ = width;
        }
    }

    TFuture<TSharedRef> Poll() override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        return ConcurrentReader_->Read()
            .WithTimeout(PollTimeout);
    }

    TShellId GetId() const override
    {
        return Id_;
    }

    int GetIndex() const override
    {
        return Index_;
    }

    ui64 SendKeys(const TSharedRef& keys, ui64 inputOffset) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

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
                    BIND(&TShellBase::Terminate, MakeWeak(this), InactivityError_)
                        .Via(GetCurrentInvoker()),
                    InactivityTimeout_);
            }
        }
        return ConsumedOffset_;
    }

    TFuture<void> Shutdown(const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (IsRunning_ && !InactivityCookie_) {
            auto delay = InactivityTimeout_;
            InactivityError_ = error;
            InactivityCookie_ = TDelayedExecutor::Submit(
                BIND(&TShellBase::Terminate, MakeWeak(this), InactivityError_)
                    .Via(GetCurrentInvoker()),
                delay);
        }
        return TerminatedPromise_;
    }

    virtual void TerminateSpecific()
    { }

    void Terminate(const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!IsRunning_) {
            return;
        }
        IsRunning_ = false;

        TDelayedExecutor::CancelAndClear(InactivityCookie_);
        YT_UNUSED_FUTURE(Writer_->Abort());
        Reader_->SetReadDeadline(TInstant::Now() + TerminatedShellReadTimeout);
        TerminatedPromise_.TrySet();

        TerminateSpecific();

        YT_LOG_INFO(error, "Shell terminated");
    }

    bool Terminated() const override
    {
        return TerminatedPromise_.IsSet();
    }

protected:
    const std::unique_ptr<TShellOptions> Options_;
    const TShellId Id_;
    const int Index_;
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

    NLogging::TLogger Logger = ShellLogger();

    std::unique_ptr<TPty> Pty_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

class TPortoShell
    : public TShellBase
{
public:
    TPortoShell(
        IPortoExecutorPtr portoExecutor,
        std::unique_ptr<TShellOptions> options)
        : TShellBase(std::move(options))
        , PortoExecutor_(std::move(portoExecutor))
    { }

    void Spawn()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!IsRunning_);
        IsRunning_ = true;

        YT_LOG_INFO("Spawning job shell container (ContainerName: %v)", Options_->ContainerName);
        auto launcher = CreatePortoInstanceLauncher(Options_->ContainerName, PortoExecutor_);

        int uid = Options_->Uid.value_or(::getuid());
        auto user = SafeGetUsernameByUid(uid);
        auto gid = Options_->Gid;
        auto preparationDir = Options_->PreparationDir;
        auto workingDir = Options_->WorkingDir;

        YT_LOG_INFO("Spawning TTY (Term: %v, Height: %v, Width: %v, Uid: %v, Username: %v, "
            "Gid: %v, PreparationDir: %v, WorkingDir: %v, InactivityTimeout: %v, Command: %v)",
            Options_->Term,
            CurrentHeight_,
            CurrentWidth_,
            uid,
            user,
            gid,
            preparationDir,
            workingDir,
            InactivityTimeout_,
            Command_);

        Pty_ = std::make_unique<TPty>(CurrentHeight_, CurrentWidth_);
        const TString tty = Format("/dev/fd/%v", Pty_->GetSlaveFD());

        launcher->SetStdIn(tty);
        launcher->SetStdOut(tty);
        launcher->SetStdErr(tty);

        // NB(gritukan, psushin): Porto is unable to resolve username inside subcontainer
        // so we pass uid instead.
        launcher->SetUser(ToString(uid));
        if (gid) {
            launcher->SetGroup(*gid);
        }

        launcher->SetEnablePorto(Options_->EnablePorto ? EEnablePorto::Full : EEnablePorto::None);
        launcher->SetIsolate(false);

        Reader_ = Pty_->CreateMasterAsyncReader();
        auto bufferingReader = CreateBufferingAdapter(Reader_, ReaderBufferSize);
        ConcurrentReader_ = CreateConcurrentAdapter(bufferingReader);

        Writer_ = Pty_->CreateMasterAsyncWriter();
        ZeroCopyWriter_ = CreateZeroCopyAdapter(Writer_);

        launcher->SetCwd(workingDir);

        // Init environment variables.
        THashMap<TString, TString> env;
        env["HOME"] = workingDir;
        env["G_HOME"] = workingDir;
        env["TMPDIR"] = NFS::CombinePaths(workingDir, "tmp");
        env["LOGNAME"] = user;
        env["USER"] = user;
        env["TERM"] = Options_->Term;
        env["LANG"] = "en_US.UTF-8";
        env["YT_SHELL_ID"] = ToString(Id_);

        for (const auto& var : Options_->Environment) {
            TStringBuf name, value;
            TStringBuf(var).TrySplit('=', name, value);
            env[name] = value;
        }

        if (Options_->MessageOfTheDay) {
            auto path = NFS::CombinePaths(preparationDir, ".motd");

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
            auto path = NFS::CombinePaths(preparationDir, ".bashrc");

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
        if (Options_->EnableJobShellSeccopm) {
            auto toolConfig = New<TSpawnShellConfig>();
            toolConfig->Command = Options_->Command;
            auto args = GenerateToolArguments<TSpawnShellTool>(toolConfig);

            Instance_ = WaitFor(launcher->Launch(ShellToolPath, args, env))
                .ValueOrThrow();
        } else {
            // COMPAT(pushin): remove me after 21.3.
            TString path("/bin/bash");
            std::vector<TString> args;
            if (Options_->Command) {
                args = {"-c", *Options_->Command};
            }

            Instance_ = WaitFor(launcher->Launch(path, args, env))
                .ValueOrThrow();
        }

        Instance_->Wait()
            .Subscribe(
                BIND(&TPortoShell::Terminate, MakeWeak(this))
                    .Via(GetCurrentInvoker()));
        YT_LOG_INFO("Shell started");
    }

    void TerminateSpecific() override
    {
        if (Instance_) {
            Instance_->Destroy();
        }
    }

private:
    const IPortoExecutorPtr PortoExecutor_;
    IInstancePtr Instance_;
};

////////////////////////////////////////////////////////////////////////////////

IShellPtr CreatePortoShell(
    NContainers::IPortoExecutorPtr portoExecutor,
    std::unique_ptr<TShellOptions> options)
{
    auto shell = New<TPortoShell>(std::move(portoExecutor), std::move(options));
    shell->Spawn();
    return shell;
}

////////////////////////////////////////////////////////////////////////////////

class TShell
    : public TShellBase
{
public:
    explicit TShell(std::unique_ptr<TShellOptions> options)
        : TShellBase(std::move(options))
        , Process_(New<TSimpleProcess>(Options_->ExePath, /*copyEnv*/ false))
    { }

    void Spawn()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(!IsRunning_);
        IsRunning_ = true;

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

        TPty pty(CurrentHeight_, CurrentWidth_);

        Reader_ = pty.CreateMasterAsyncReader();
        auto bufferingReader = CreateBufferingAdapter(Reader_, ReaderBufferSize);
        auto expiringReader = CreateExpiringAdapter(bufferingReader, PollTimeout);
        ConcurrentReader_ = CreateConcurrentAdapter(expiringReader);

        Writer_ = pty.CreateMasterAsyncWriter();
        ZeroCopyWriter_ = CreateZeroCopyAdapter(Writer_);

        Process_->SetWorkingDirectory(home);

        if (Options_->MessageOfTheDay) {
            auto path = NFS::CombinePaths(home, ".motd");

            try {
                TFile file(path, CreateAlways | WrOnly | Seq | CloseOnExec);
                TUnbufferedFileOutput output(file);
                output.Write(*Options_->MessageOfTheDay);
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
                output.Write(*Options_->Bashrc);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error saving shell config file")
                    << ex
                    << TErrorAttribute("path", path);
            }
        }

        {
            auto executorConfig = New<NUserJob::TUserJobExecutorConfig>();

            if (Options_->Command) {
                executorConfig->Command = *Options_->Command;
            }

            executorConfig->Pty = pty.GetSlaveFD();
            executorConfig->Environment.reserve(Options_->Environment.size() + 6);

            executorConfig->Environment.push_back("HOME=" + home);
            executorConfig->Environment.push_back("LOGNAME=" + user);
            executorConfig->Environment.push_back("USER=" + user);
            executorConfig->Environment.push_back("TERM=" + Options_->Term);
            executorConfig->Environment.push_back("LANG=en_US.UTF-8");
            executorConfig->Environment.push_back("YT_SHELL_ID=" + ToString(Id_));

            for (const auto& var : Options_->Environment) {
                executorConfig->Environment.push_back(var);
            }

            executorConfig->StderrPath = Format("../stderr.%v", Id_);
            auto executorConfigPath = NFS::CombinePaths(home, Format("../executor.%v.yson", Id_));

            try {
                TFile configFile(executorConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
                TUnbufferedFileOutput output(configFile);
                NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty);
                Serialize(executorConfig, &writer);
                writer.Flush();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to write executor config into %v", executorConfigPath) << ex;
            }

            Process_->AddArguments({"--config", executorConfigPath});
        }
        ResizeWindow(CurrentHeight_, CurrentWidth_);
        Process_->Spawn().Subscribe(BIND(&TShell::Terminate, MakeWeak(this)).Via(GetCurrentInvoker()));
        YT_LOG_INFO("Shell started");
    }

private:
    const TProcessBasePtr Process_;
};

////////////////////////////////////////////////////////////////////////////////

IShellPtr CreateShell(std::unique_ptr<TShellOptions> options)
{
    auto shell = New<TShell>(std::move(options));
    shell->Spawn();
    return shell;
}

#else

IShellPtr CreatePortoShell(
    NContainers::IPortoExecutorPtr /*portoExecutor*/,
    std::unique_ptr<TShellOptions> /*options*/)
{
    THROW_ERROR_EXCEPTION("Shell is supported only under Unix");
}

IShellPtr CreateShell(std::unique_ptr<TShellOptions> /*options*/)
{
    THROW_ERROR_EXCEPTION("Shell is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
