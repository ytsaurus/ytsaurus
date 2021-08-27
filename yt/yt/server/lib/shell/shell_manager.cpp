#include "shell_manager.h"
#include "shell.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/containers/instance.h>

#ifdef __linux__
#include <yt/yt/server/lib/containers/porto_executor.h>
#endif

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/library/process/public.h>

#include <yt/yt/core/net/public.h>

#include <util/string/hex.h>

#include <util/system/execpath.h>
#include <util/system/fs.h>

namespace NYT::NShell {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NContainers;
using namespace NJobProberClient;

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
        IPortoExecutorPtr portoExecutor,
        IInstancePtr rootInstance,
        const TString& preparationDir,
        const TString& workingDir,
        std::optional<int> userId,
        std::optional<int> groupId,
        std::optional<TString> messageOfTheDay,
        std::vector<TString> environment)
        : PortoExecutor_(std::move(portoExecutor))
        , RootInstance_(std::move(rootInstance))
        , PreparationDir_(preparationDir)
        , WorkingDir_(workingDir)
        , UserId_(userId)
        , GroupId_(groupId)
        , MessageOfTheDay_(messageOfTheDay)
        , Environment_(std::move(environment))
    { }

    TYsonString PollJobShell(
        const TJobShellDescriptor& jobShellDescriptor,
        const TYsonString& serializedParameters) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TShellParameters parameters;
        TShellResult result;
        IShellPtr shell;

        Deserialize(parameters, ConvertToNode(serializedParameters));
        if (parameters.Operation != EShellOperation::Spawn) {
            shell = GetShellOrThrow(parameters.ShellId, parameters.ShellIndex);
        }
        if (Terminated_) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::ShellManagerShutDown,
                "Shell manager was shut down");
        }

        switch (parameters.Operation) {
            case EShellOperation::Spawn: {
                auto options = std::make_unique<TShellOptions>();
                if (parameters.Term && !parameters.Term->empty()) {
                    options->Term = *parameters.Term;
                }
                options->Uid = UserId_;
                options->Gid = GroupId_;
                if (parameters.Height != 0) {
                    options->Height = parameters.Height;
                }
                if (options->Width != 0) {
                    options->Width = parameters.Width;
                }
                Environment_.insert(
                    Environment_.end(),
                    parameters.Environment.begin(),
                    parameters.Environment.end());
                options->Environment = Environment_;
                options->PreparationDir = PreparationDir_;
                if (parameters.Command) {
                    options->Command = parameters.Command;
                } else {
                    auto bashrc = TString{Bashrc};
                    for (const auto& variable : Environment_) {
                        if (variable.StartsWith("PS1=")) {
                            bashrc = Format("export %v\n%v", variable, bashrc);
                        }
                    }
                    options->Bashrc = bashrc;
                    options->MessageOfTheDay = MessageOfTheDay_;
                    options->InactivityTimeout = parameters.InactivityTimeout;
                }
                options->Id = TGuid::Create();
                options->Index = NextShellIndex_++;
                auto subcontainerName = RootInstance_->GetName() + jobShellDescriptor.Subcontainer;
                options->ContainerName = Format("%v/js-%v", subcontainerName, options->Index);
#ifdef _linux_
                options->ContainerUser = *WaitFor(PortoExecutor_->GetContainerProperty(subcontainerName, "user"))
                    .ValueOrThrow();
                {
                    auto enablePorto = WaitFor(
                        PortoExecutor_->GetContainerProperty(
                            subcontainerName,
                            "enable_porto"))
                        .ValueOrThrow();
                    if (enablePorto && enablePorto != "none" && enablePorto != "false") {
                        options->EnablePorto = true;
                    }
                }

                if (!jobShellDescriptor.Subcontainer.empty()) {
                    options->WorkingDir = *WaitFor(PortoExecutor_->GetContainerProperty(subcontainerName, "cwd"))
                        .ValueOrThrow();
                } else {
                    options->WorkingDir = WorkingDir_;
                }
#endif

                shell = CreateShell(PortoExecutor_, std::move(options));
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
                    if (shell->Terminated()) {
                        THROW_ERROR_EXCEPTION(EErrorCode::ShellExited, "Shell exited")
                            << TErrorAttribute("shell_id", parameters.ShellId)
                            << TErrorAttribute("shell_index", parameters.ShellIndex);
                    }
                    result.Output = "";
                    break;
                }
                if (pollResult.FindMatching(NNet::EErrorCode::Aborted)) {
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::ShellManagerShutDown,
                        "Shell manager was shut down")
                        << TErrorAttribute("shell_id", parameters.ShellId)
                        << TErrorAttribute("shell_index", parameters.ShellIndex)
                        << pollResult;
                }
                if (!pollResult.IsOK() || pollResult.Value().Empty()) {
                    THROW_ERROR_EXCEPTION(EErrorCode::ShellExited, "Shell exited")
                        << TErrorAttribute("shell_id", parameters.ShellId)
                        << TErrorAttribute("shell_index", parameters.ShellIndex)
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
        result.ShellIndex = shell->GetIndex();
        return ConvertToYsonString(result);
    }

    void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Shell manager is terminating");
        Terminated_ = true;
        for (auto& shell : IdToShell_) {
            shell.second->Terminate(error);
        }
        IdToShell_.clear();
        IndexToShell_.clear();
    }

    TFuture<void> GracefulShutdown(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Shell manager is shutting down");
        std::vector<TFuture<void>> futures;
        for (auto& shell : IdToShell_) {
            futures.push_back(shell.second->Shutdown(error));
        }
        return AllSet(futures).As<void>();
    }

private:
    const IPortoExecutorPtr PortoExecutor_;
    const IInstancePtr RootInstance_;
    const TString PreparationDir_;
    const TString WorkingDir_;
    std::optional<int> UserId_;
    std::optional<int> GroupId_;
    std::optional<TString> MessageOfTheDay_;

    std::vector<TString> Environment_;
    THashMap<TShellId, IShellPtr> IdToShell_;
    THashMap<int, IShellPtr> IndexToShell_;
    bool Terminated_ = false;

    int NextShellIndex_ = 1;

    const NLogging::TLogger Logger = ShellLogger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void Register(IShellPtr shell)
    {
        YT_VERIFY(IdToShell_.emplace(shell->GetId(), shell).second);
        YT_VERIFY(IndexToShell_.emplace(shell->GetIndex(), shell).second);

        YT_LOG_DEBUG("Shell registered (ShellId: %v, ShellIndex: %v)",
            shell->GetId(),
            shell->GetIndex());
    }

    IShellPtr Find(TShellId shellId) const
    {
        auto it = IdToShell_.find(shellId);
        return it == IdToShell_.end() ? nullptr : it->second;
    }

    IShellPtr Find(int shellIndex) const
    {
        auto it = IndexToShell_.find(shellIndex);
        return it == IndexToShell_.end() ? nullptr : it->second;
    }

    IShellPtr GetShellOrThrow(
        std::optional<TShellId> shellId,
        std::optional<int> shellIndex)
    {
        if (shellId) {
            auto shell = Find(*shellId);
            if (shell) {
                return shell;
            }
        }

        if (shellIndex) {
            auto shell = Find(*shellIndex);
            if (shell) {
                return shell;
            }
        }

        THROW_ERROR_EXCEPTION("No such shell %v", shellId);
    }
};

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreateShellManager(
    IPortoExecutorPtr portoExecutor,
    IInstancePtr rootInstance,
    const TString& preparationDir,
    const TString& workingDir,
    std::optional<int> userId,
    std::optional<int> groupId,
    std::optional<TString> messageOfTheDay,
    std::vector<TString> environment)
{
    return New<TShellManager>(
        std::move(portoExecutor),
        std::move(rootInstance),
        preparationDir,
        workingDir,
        userId,
        groupId,
        messageOfTheDay,
        std::move(environment));
}

#else

IShellManagerPtr CreateShellManager(
    IPortoExecutorPtr portoExecutor,
    IInstancePtr rootInstance,
    const TString& preparationDir,
    const TString& workingDir,
    std::optional<int> userId,
    std::optional<int> groupId,
    std::optional<TString> messageOfTheDay,
    std::vector<TString> environment)
{
    THROW_ERROR_EXCEPTION("Shell manager is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
