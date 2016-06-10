#include "shell.h"
#include "private.h"

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/core/actions/bind.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/process.h>

#include <yt/core/pipes/async_reader.h>
#include <yt/core/pipes/async_writer.h>
#include <yt/core/pipes/pty.h>

#include <util/stream/file.h>

namespace NYT {
namespace NShell {

using namespace NConcurrency;
using namespace NCGroup;
using namespace NPipes;

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
        , Process_(New<TProcess>(Options_->ExePath, false))
        , Freezer_(Options_->CGroupBasePath.Get("") + CGroupShellPrefix + ToString(Id_))
        , CurrentHeight_(Options_->Height)
        , CurrentWidth_(Options_->Width)
    {
        Logger.AddTag("ShellId: %v", Id_);
    }

    void Spawn()
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);

            YCHECK(!IsRunning_);
            IsRunning_ = true;
        }

        int uid = Options_->Uid.Get(::getuid());
        auto user = SafeGetUsernameByUid(uid);
        auto home = Options_->WorkingDir;

        LOG_INFO("Spawning TTY (Term: %v, Height: %v, Width: %v, Uid: %v, Username: %v, Home: %v)",
            Options_->Term,
            CurrentHeight_,
            CurrentWidth_,
            uid,
            user,
            home);

        TPty pty(CurrentHeight_, CurrentWidth_);

        Reader_ = pty.CreateMasterAsyncReader();
        auto bufferingReader = CreateBufferingAdapter(Reader_, ReaderBufferSize);
        auto expiringReader = CreateExpiringAdapter(bufferingReader, PollTimeout);
        ConcurrentReader_ = CreateConcurrentAdapter(expiringReader);

        Writer_ = pty.CreateMasterAsyncWriter();
        ZeroCopyWriter_ = CreateZeroCopyAdapter(Writer_);

        Process_->AddArguments({"--shell", ::ToString(pty.GetSlaveFD())});
        Process_->AddArguments({"--working-dir", home});
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
            Process_->AddArguments({ "--env", var });
        }

        if (Options_->MessageOfTheDay) {
            auto path = NFS::CombinePaths(home, ".motd");

            try {
                TFile file(path, CreateAlways | WrOnly | Seq | CloseOnExec);
                TFileOutput output(file);
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
                TFileOutput output(file);
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
                BIND(&TShell::Terminate, MakeWeak(this)));
        LOG_INFO("Shell started");
    }

    virtual void ResizeWindow(int height, int width) override
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (CurrentHeight_ != height || CurrentWidth_ != width) {
            SafeSetTtyWindowSize(Reader_->GetHandle(), height, width);
            CurrentHeight_ = height;
            CurrentWidth_ = width;
        }
    }

    virtual ui64 SendKeys(const TSharedRef& keys, ui64 inputOffset) override
    {
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
        }
        return ConsumedOffset_;
    }

    virtual TFuture<TSharedRef> Poll() override
    {
        return ConcurrentReader_->Read();
    }

    virtual void Terminate(const TError& error) override
    {
        LOG_INFO(error, "Shell terminated");
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!IsRunning_) {
                return;
            }
            IsRunning_ = false;
        }
        Writer_->Abort();
        Reader_->Abort();
        CleanupShellProcesses();
    }

    virtual const TShellId& GetId() override
    {
        return Id_;
    }

private:
    const std::unique_ptr<TShellOptions> Options_;
    const TShellId Id_;
    TProcessPtr Process_;
    TFreezer Freezer_;
    int CurrentHeight_;
    int CurrentWidth_;

    bool IsRunning_ = false;
    TSpinLock SpinLock_;

    TAsyncWriterPtr Writer_;
    IAsyncZeroCopyOutputStreamPtr ZeroCopyWriter_;
    ui64 ConsumedOffset_ = 0;

    TAsyncReaderPtr Reader_;
    IAsyncZeroCopyInputStreamPtr ConcurrentReader_;

    NLogging::TLogger Logger = ShellLogger;

    void PrepareCGroups()
    {
        if (!Options_->CGroupBasePath) {
            return;
        }

        try {
            TGuard<TSpinLock> guard(SpinLock_);
            Freezer_.Create();
            Process_->AddArguments({ "--cgroup", Freezer_.GetFullPath() });
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create required cgroups");
        }
    }

    void CleanupShellProcesses()
    {
        if (!Options_->CGroupBasePath) {
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

} // namespace NShell
} // namespace NYT
