#include "shell_manager.h"
#include "shell.h"
#include "private.h"
#include "config.h"

#include <yt/server/misc/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/pipes/public.h>

#include <yt/core/net/public.h>

#include <util/string/hex.h>

#include <util/system/execpath.h>
#include <util/system/fs.h>

namespace NYT {
namespace NShell {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_

// G_HOME environment variable is used by utilities based on glib2 (e.g. Midnight Commander),
// to override place where settings and cache data are stored
// (normally ~/.local and ~/.cache directories).
// If not specified, these directories are located in user's home directory from /etc/passwd,
// but that directory may be unaccessible in sandbox environment.
// TMPDIR is used to specify a separate temp directory instead of common one.
// TMOUT is a inactivity timeout (in seconds) to exit the shell.
static const char* Bashrc =
    "export PATH\n"
    "mkdir -p \"$TMPDIR\"\n"
    "stty sane ignpar iutf8\n"
    "TMOUT=1800\n"
    "alias cp='cp -i'\n"
    "alias mv='mv -i'\n"
    "alias rm='rm -i'\n"
    "alias perf_top='sudo /usr/bin/perf top -u \"$USER\"'\n"
    "echo\n"
    "[ -f .motd ] && cat .motd\n"
    "echo\n"
    "ps -fu `id -u` --forest\n"
    "echo\n";

////////////////////////////////////////////////////////////////////////////////

class TShellManager
    : public IShellManager
{
public:
    TShellManager(
        const TString& workingDir,
        std::optional<int> userId,
        std::optional<TString> freezerFullPath,
        std::optional<TString> messageOfTheDay,
        std::vector<TString> environment)
        : WorkingDir_(workingDir)
        , UserId_(userId)
        , FreezerFullPath_(freezerFullPath)
        , MessageOfTheDay_(messageOfTheDay)
        , Environment_(std::move(environment))
    {
        Environment_.emplace_back(Format("HOME=%v", WorkingDir_));
        Environment_.emplace_back(Format("G_HOME=%v", WorkingDir_));
        auto tmpDirPath = NFS::CombinePaths(WorkingDir_, "tmp");
        Environment_.emplace_back(Format("TMPDIR=%v", tmpDirPath));
    }

    virtual TYsonString PollJobShell(const TYsonString& serializedParameters) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TShellParameters parameters;
        TShellResult result;
        IShellPtr shell;

        Deserialize(parameters, ConvertToNode(serializedParameters));
        if (parameters.Operation != EShellOperation::Spawn) {
            shell = GetShellOrThrow(parameters.ShellId);
        }
        if (Terminated_) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::ShellManagerShutDown,
                "Shell manager was shut down");
        }

        switch (parameters.Operation) {
            case EShellOperation::Spawn: {
                auto options = std::make_unique<TShellOptions>();
                options->ExePath = ExecProgramName;
                if (parameters.Term && !parameters.Term->empty()) {
                    options->Term = *parameters.Term;
                }
                options->Uid = UserId_;
                if (parameters.Height != 0) {
                    options->Height = parameters.Height;
                }
                if (options->Width != 0) {
                    options->Width = parameters.Width;
                }
                options->CGroupBasePath = FreezerFullPath_;
                Environment_.insert(
                    Environment_.end(),
                    parameters.Environment.begin(),
                    parameters.Environment.end());
                options->Environment = Environment_;
                options->WorkingDir = WorkingDir_;
                if (parameters.Command) {
                    options->Command = parameters.Command;
                } else {
                    options->Bashrc = Bashrc;
                    options->MessageOfTheDay = MessageOfTheDay_;
                    options->InactivityTimeout = parameters.InactivityTimeout;
                }

                shell = CreateShell(std::move(options));
                Register(shell);
                shell->ResizeWindow(parameters.Height, parameters.Width);
                break;
            }

            case EShellOperation::Update: {
                shell->ResizeWindow(parameters.Height, parameters.Width);
                if (!parameters.Keys.empty()) {
                    result.ConsumedOffset = shell->SendKeys(
                        TSharedRef::FromString(HexDecode(parameters.Keys)),
                        *parameters.InputOffset);
                }
                break;
            }

            case EShellOperation::Poll: {
                auto pollResult = WaitFor(shell->Poll());
                if (pollResult.FindMatching(NYT::EErrorCode::Timeout)) {
                    result.Output = "";
                    break;
                }
                if (pollResult.FindMatching(NNet::EErrorCode::Aborted)) {
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::ShellManagerShutDown,
                        "Shell manager was shut down")
                        << TErrorAttribute("shell_id", parameters.ShellId)
                        << pollResult;
                }
                if (!pollResult.IsOK() || pollResult.Value().Empty()) {
                    THROW_ERROR_EXCEPTION(EErrorCode::ShellExited, "Shell exited")
                        << TErrorAttribute("shell_id", parameters.ShellId)
                        << pollResult;
                }
                result.Output = ToString(pollResult.Value());
                break;
            }

            case EShellOperation::Terminate: {
                shell->Terminate(TError("Shell %v terminated by user request", shell->GetId()));
                break;
            }

            default:
                THROW_ERROR_EXCEPTION(
                    "Unknown operation %Qlv for shell %v",
                    parameters.Operation,
                    parameters.ShellId);
        }

        result.ShellId = shell->GetId();
        return ConvertToYsonString(result);
    }

    virtual void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Shell manager is terminating");
        Terminated_ = true;
        for (auto& shell : IdToShell_) {
            shell.second->Terminate(error);
        }
        IdToShell_.clear();
    }

    virtual TFuture<void> GracefulShutdown(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Shell manager is shutting down");
        std::vector<TFuture<void>> futures;
        for (auto& shell : IdToShell_) {
            futures.push_back(shell.second->Shutdown(error));
        }
        return CombineAll(futures).As<void>();
    }

private:
    const TString WorkingDir_;
    std::optional<int> UserId_;
    std::optional<TString> FreezerFullPath_;
    std::optional<TString> MessageOfTheDay_;

    std::vector<TString> Environment_;
    THashMap<TShellId, IShellPtr> IdToShell_;
    bool Terminated_ = false;

    const NLogging::TLogger Logger = ShellLogger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void Register(IShellPtr shell)
    {
        YCHECK(IdToShell_.insert(std::make_pair(shell->GetId(), shell)).second);

        LOG_DEBUG("Shell registered (ShellId: %v)",
            shell->GetId());
    }

    IShellPtr Find(const TShellId& shellId)
    {
        auto it = IdToShell_.find(shellId);
        return it == IdToShell_.end() ? nullptr : it->second;
    }

    IShellPtr GetShellOrThrow(const TShellId& shellId)
    {
        auto shell = Find(shellId);
        if (!shell) {
            THROW_ERROR_EXCEPTION("No such shell %v", shellId);
        }
        return shell;
    }
};

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreateShellManager(
    const TString& workingDir,
    std::optional<int> userId,
    std::optional<TString> freezerFullPath,
    std::optional<TString> messageOfTheDay,
    std::vector<TString> environment)
{
    return New<TShellManager>(workingDir, userId, freezerFullPath, messageOfTheDay, std::move(environment));
}

#else

IShellManagerPtr CreateShellManager(
    const TString& workingDir,
    std::optional<int> userId,
    std::optional<TString> freezerFullPath,
    std::optional<TString> messageOfTheDay,
    std::vector<TString> environment)
{
    THROW_ERROR_EXCEPTION("Shell manager is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NShell
} // namespace NYT
