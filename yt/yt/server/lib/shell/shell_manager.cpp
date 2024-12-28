#include "shell_manager.h"
#include "shell.h"
#include "private.h"
#include "config.h"
#include "yt/yt/core/misc/error.h"

#include <yt/yt/library/containers/instance.h>

#include <yt/yt/server/lib/exec_node/public.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#ifdef __linux__
#include <yt/yt/library/containers/porto_executor.h>
#endif

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/process/public.h>
#include <yt/yt/library/process/process.h>

#include <util/string/hex.h>

#include <util/system/execpath.h>
#include <util/system/fs.h>

namespace NYT::NShell {

using namespace NApi;
using namespace NConcurrency;
using namespace NContainers;
using namespace NExecNode;
using namespace NJobProberClient;
using namespace NTools;
using namespace NYTree;
using namespace NYson;
using namespace NFS;
using namespace NServer;

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

class TShellManagerBase
    : public IShellManager
{
public:
    TShellManagerBase(const TShellManagerConfig& config)
        : PreparationDir_(CombinePaths(config.PreparationDir, GetSandboxRelPath(ESandboxKind::Home)))
        , WorkingDir_(CombinePaths(config.WorkingDir, GetSandboxRelPath(ESandboxKind::Home)))
        , EnableJobShellSeccopm(config.EnableJobShellSeccopm)
        , UserId_(config.UserId)
        , GroupId_(config.GroupId)
        , MessageOfTheDay_(config.MessageOfTheDay)
        , Environment_(config.Environment)
    { }

    virtual IShellPtr MakeShell(std::unique_ptr<TShellOptions> options, const TJobShellDescriptor& jobShellDescriptor) = 0;

    TPollJobShellResponse PollJobShell(
        const TJobShellDescriptor& jobShellDescriptor,
        const TYsonString& serializedParameters) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TShellParameters parameters;
        IShellPtr shell;
        TShellResult resultValue;
        TYsonString loggingContext;

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

                loggingContext = BuildYsonStringFluently<EYsonType::MapFragment>(EYsonFormat::Text)
                    .Item("shell_id").Value(options->Id)
                    .Finish();

                options->Index = NextShellIndex_++;
                options->WorkingDir = WorkingDir_;

                shell = MakeShell(std::move(options), jobShellDescriptor);

                Register(shell);
                shell->ResizeWindow(parameters.Height, parameters.Width);

                break;
            }

            case EShellOperation::Update: {
                shell->ResizeWindow(parameters.Height, parameters.Width);
                if (!parameters.Keys.empty()) {
                    resultValue.ConsumedOffset = shell->SendKeys(
                        TSharedRef::FromString(HexDecode(parameters.Keys)),
                        *parameters.InputOffset);
                }
                break;
            }

            case EShellOperation::Poll: {
                auto pollResult = WaitFor(shell->Poll());
                if (pollResult.FindMatching(NYT::EErrorCode::Timeout)) {
                    if (shell->Terminated()) {
                        THROW_ERROR_EXCEPTION(NShell::EErrorCode::ShellExited, "Shell exited")
                            << TErrorAttribute("shell_id", parameters.ShellId)
                            << TErrorAttribute("shell_index", parameters.ShellIndex);
                    }
                    resultValue.Output = "";
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
                    THROW_ERROR_EXCEPTION(NShell::EErrorCode::ShellExited, "Shell exited")
                        << TErrorAttribute("shell_id", parameters.ShellId)
                        << TErrorAttribute("shell_index", parameters.ShellIndex)
                        << pollResult;
                }
                resultValue.Output = ToString(pollResult.Value());
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

        resultValue.ShellId = shell->GetId();
        resultValue.ShellIndex = shell->GetIndex();
        return TPollJobShellResponse {
            .Result = ConvertToYsonString(resultValue),
            .LoggingContext = loggingContext,
        };
    }

    void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Shell manager is terminating");

        Terminated_ = true;
        for (const auto& [_, shell] : IdToShell_) {
            shell->Terminate(error);
        }
        IdToShell_.clear();
        IndexToShell_.clear();
    }

    TFuture<void> GracefulShutdown(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Shell manager is shutting down");

        std::vector<TFuture<void>> futures;
        futures.reserve(IdToShell_.size());
        for (const auto& [_, shell] : IdToShell_) {
            futures.push_back(shell->Shutdown(error));
        }
        return AllSucceeded(futures);
    }

protected:
    const TString PreparationDir_;
    const TString WorkingDir_;
    const bool EnableJobShellSeccopm;
    std::optional<int> UserId_;
    std::optional<int> GroupId_;
    std::optional<TString> MessageOfTheDay_;

    std::vector<TString> Environment_;
    THashMap<TShellId, IShellPtr> IdToShell_;
    THashMap<int, IShellPtr> IndexToShell_;
    bool Terminated_ = false;

    int NextShellIndex_ = 1;

    const NLogging::TLogger Logger = ShellLogger();

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void Register(const IShellPtr& shell)
    {
        EmplaceOrCrash(IdToShell_, shell->GetId(), shell);
        EmplaceOrCrash(IndexToShell_, shell->GetIndex(), shell);

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
            if (auto shell = Find(*shellId)) {
                return shell;
            }
        }

        if (shellIndex) {
            if (auto shell = Find(*shellIndex)) {
                return shell;
            }
        }

        THROW_ERROR_EXCEPTION("No such shell %Qv", shellId);
    }
};

class TPortoShellManager
    : public TShellManagerBase
{
public:
    TPortoShellManager(
        TShellManagerConfig config,
        IPortoExecutorPtr portoExecutor,
        IInstancePtr rootInstance)
        : TShellManagerBase(std::move(config))
        , RootInstance_(std::move(rootInstance))
        , PortoExecutor_(std::move(portoExecutor))
    { }

    IShellPtr MakeShell(std::unique_ptr<TShellOptions> options, const TJobShellDescriptor& jobShellDescriptor) override
    {
        options->EnableJobShellSeccopm = EnableJobShellSeccopm;

        auto subcontainerName = (RootInstance_ ? RootInstance_->GetName() : "") + jobShellDescriptor.Subcontainer;
        options->ContainerName = Format("%v/js-%v", subcontainerName, options->Index);
#ifdef _linux_
        options->ContainerUser = *WaitFor(PortoExecutor_->GetContainerProperty(subcontainerName, "user")).ValueOrThrow();
        {
            auto enablePorto = WaitFor(PortoExecutor_->GetContainerProperty(subcontainerName, "enable_porto")).ValueOrThrow();
            if (enablePorto && enablePorto != "none" && enablePorto != "false") {
                options->EnablePorto = true;
            }
        }

        if (!jobShellDescriptor.Subcontainer.empty()) {
            options->WorkingDir = *WaitFor(PortoExecutor_->GetContainerProperty(subcontainerName, "cwd")).ValueOrThrow();
        } else {
            options->WorkingDir = WorkingDir_;
        }

        if (EnableJobShellSeccopm) {
            EnsureToolBinaryPath(subcontainerName);
        }
#endif

        return CreatePortoShell(PortoExecutor_, std::move(options));
    }


private:
    const IInstancePtr RootInstance_;
    const IPortoExecutorPtr PortoExecutor_;

#ifdef _linux_
    void EnsureToolBinaryPath(const TString& container) const
    {
        auto containerRoot = WaitFor(PortoExecutor_->ConvertPath("/", container))
            .ValueOrThrow();

        YT_LOG_DEBUG("Preparing shell tool path (Container: %v, ContainerRoot: %v)",
            container,
            containerRoot);

        auto toolDirectory = JoinPaths(containerRoot, ShellToolDirectory);
        if (!Exists(toolDirectory)) {
            RunTool<TCreateDirectoryAsRootTool>(toolDirectory);
            auto toolPathOrError = ResolveBinaryPath(TString(NTools::ToolsProgramName));
        }

        if (IsDirEmpty(toolDirectory)) {
            auto toolPathOrError = ResolveBinaryPath(TString(NTools::ToolsProgramName));
            THROW_ERROR_EXCEPTION_IF_FAILED(toolPathOrError, "Failed to resolve tool binary path");

            THashMap<TString, TString> volumeProperties;
            volumeProperties["backend"] = "bind";
            volumeProperties["storage"] = GetDirectoryName(toolPathOrError.Value());

            auto pathOrError = WaitFor(PortoExecutor_->CreateVolume(toolDirectory, volumeProperties));
            THROW_ERROR_EXCEPTION_IF_FAILED(pathOrError, "Failed to bind tools inside job shell")
        }
    }
#endif
};

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreatePortoShellManager(
    TShellManagerConfig config,
    IPortoExecutorPtr portoExecutor,
    IInstancePtr rootInstance)
{
    return New<TPortoShellManager>(
        config,
        std::move(portoExecutor),
        std::move(rootInstance));
}

class TShellManager
    : public TShellManagerBase
{
public:
    explicit TShellManager(const TShellManagerConfig& config)
        : TShellManagerBase(config)
    {
        Environment_.emplace_back(Format("HOME=%v", WorkingDir_));
        Environment_.emplace_back(Format("G_HOME=%v", WorkingDir_));
        auto tmpDirPath = NFS::CombinePaths(WorkingDir_, "tmp");
        Environment_.emplace_back(Format("TMPDIR=%v", tmpDirPath));
    }

    IShellPtr MakeShell(
        std::unique_ptr<TShellOptions> options, [[maybe_unused]] const TJobShellDescriptor& jobShellDescriptor) override
    {
        options->ExePath = ExecProgramName;
        return CreateShell(std::move(options));
    }

    void Terminate(const TError& error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Shell manager is terminating");
        Terminated_ = true;
        for (const auto& [_, shell] : IdToShell_) {
            shell->Terminate(error);
        }
        IdToShell_.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

IShellManagerPtr CreateShellManager(TShellManagerConfig config)
{
     return New<TShellManager>(std::move(config));
}

#else

IShellManagerPtr CreatePortoShellManager(
    TShellManagerConfig /*config*/,
    IPortoExecutorPtr /*portoExecutor*/,
    IInstancePtr /*rootInstance*/)
{
    THROW_ERROR_EXCEPTION("Shell manager is supported only under Unix");
}

IShellManagerPtr CreateShellManager(TShellManagerConfig /*config*/)
{
    THROW_ERROR_EXCEPTION("Shell manager is supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
