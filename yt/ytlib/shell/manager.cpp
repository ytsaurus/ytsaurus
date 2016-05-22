#include "manager.h"
#include "private.h"

#include <yt/ytlib/shell/shell.h>

#include <yt/core/misc/fs.h>

#include <yt/core/ytree/yson_serializable.h>

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
    "stty sane ignpar iutf8\n"
    "TMOUT=1800\n"
    "alias cp='cp -i'\n"
    "alias mv='mv -i'\n"
    "alias rm='rm -i'\n"
    "export TMPDIR=\"$HOME/tmp\"\n"
    "mkdir -p \"$TMPDIR\"\n"
    "export G_HOME=\"$HOME\"\n"
    "echo\n"
    "env | grep YT_ | sort\n"
    "echo\n"
    "ps -fu `id -u` --forest\n"
    "echo\n";

////////////////////////////////////////////////////////////////////////////////

struct TShellParameters
    : public TYsonSerializableLite
{
    TShellId ShellId;
    EShellOperation Operation;
    TNullable<Stroka> Term;
    Stroka Keys;
    int Height;
    int Width;

    TShellParameters()
    {
        RegisterParameter("shell_id", ShellId)
            .Optional();
        RegisterParameter("operation", Operation);
        RegisterParameter("term", Term)
            .Optional();
        RegisterParameter("keys", Keys)
            .Default();
        RegisterParameter("height", Height)
            .Default(0);
        RegisterParameter("width", Width)
            .Default(0);

        RegisterValidator([&] () {
            if (Operation != EShellOperation::Spawn && !ShellId) {
                THROW_ERROR_EXCEPTION(
                    "Malformed request: shell id is not specified for %Qlv operation",
                    Operation);
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TShellResult
    : public TYsonSerializableLite
{
    TShellId ShellId;
    Stroka Output;

    TShellResult()
    {
        RegisterParameter("shell_id", ShellId);
        RegisterParameter("output", Output);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TShellManager
    : public IShellManager
{
public:
    TShellManager(
        const Stroka& workingDir,
        TNullable<int> userId,
        TNullable<Stroka> freezerFullPath)
        : WorkingDir_(workingDir)
        , UserId_(userId)
        , FreezerFullPath_(freezerFullPath)
    { }

    virtual TYsonString PollJobShell(const TYsonString& serializedParameters) override
    {
        TShellParameters parameters;
        TShellResult result;
        IShellPtr shell;

        Deserialize(parameters, ConvertToNode(serializedParameters));
        if (parameters.Operation != EShellOperation::Spawn) {
            shell = GetShellOrThrow(parameters.ShellId);
        }

        switch (parameters.Operation) {
            case EShellOperation::Spawn: {
                auto options = std::make_unique<TShellOptions>();
                options->ExePath = GetExecPath();
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
                options->Environment = Environment_;
                options->WorkingDir = WorkingDir_;
                options->Bashrc = Bashrc;

                shell = CreateShell(std::move(options));
                Register(shell);
            }

            case EShellOperation::Update: {
                shell->ResizeWindow(parameters.Height, parameters.Width);
                shell->SendKeys(TSharedRef::FromString(HexDecode(parameters.Keys)));
                break;
            }

            case EShellOperation::Poll: {
                auto pollResult = WaitFor(shell->Poll());
                if (pollResult.GetCode() == NYT::EErrorCode::Timeout) {
                    result.Output = "";
                    break;
                }
                THROW_ERROR_EXCEPTION_IF_FAILED(pollResult, "Failed to poll shell %v", shell->GetId());
                if (pollResult.Value().Empty()) {
                    THROW_ERROR_EXCEPTION("Shell %v disconnected", shell->GetId());
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

    virtual void CleanupProcesses() override
    {
        yhash_map<TShellId, IShellPtr> shells;
        {
            TGuard<TSpinLock> guard(IdToShellLock_);
            std::swap(shells, IdToShell_);
        }
        for (auto& shell : shells) {
            shell.second->Terminate(TError("Job finished"));
        }
    }

    virtual void AddEnvironment(const Stroka& var) override
    {
        Environment_.push_back(var);
    }

private:
    const Stroka WorkingDir_;
    TNullable<int> UserId_;
    TNullable<Stroka> FreezerFullPath_;

    std::vector<Stroka> Environment_;
    yhash_map<TShellId, IShellPtr> IdToShell_;
    TSpinLock IdToShellLock_;

    const NLogging::TLogger Logger = ShellLogger;


    void Register(IShellPtr shell)
    {
        TGuard<TSpinLock> guard(IdToShellLock_);
        YCHECK(IdToShell_.insert(std::make_pair(shell->GetId(), shell)).second);

        LOG_DEBUG("Shell registered (ShellId: %v)",
            shell->GetId());
    }

    IShellPtr Find(const TShellId& shellId)
    {
        TGuard<TSpinLock> guard(IdToShellLock_);
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
    const Stroka& workingDir,
    TNullable<int> userId,
    TNullable<Stroka> freezerFullPath)
{
    return New<TShellManager>(workingDir, userId, freezerFullPath);
}

#else

IShellManagerPtr CreateShellManager(
    const Stroka& workingDir,
    TNullable<int> userId,
    TNullable<Stroka> freezerFullPath)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Unix");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NShell
} // namespace NYT
