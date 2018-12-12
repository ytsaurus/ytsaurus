#include "process.h"
#include "proc.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/finally.h>
#include <yt/core/pipes/pipe.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <util/folder/dirut.h>

#include <util/generic/guid.h>

#include <util/string/ascii.h>

#include <util/string/util.h>

#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/maxlen.h>
#include <util/system/shellcommand.h>

#ifdef _unix_
  #include <unistd.h>
  #include <errno.h>
  #include <sys/wait.h>
#endif

#ifdef _darwin_
  #include <crt_externs.h>
  #define environ (*_NSGetEnviron())
#endif

namespace NYT {

using namespace NPipes;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Process");

static const pid_t InvalidProcessId = -1;

static const int ExecveRetryCount = 5;
static const auto ExecveRetryTimeout = TDuration::Seconds(1);

static const int ResolveRetryCount = 5;
static const auto ResolveRetryTimeout = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TString> ResolveBinaryPath(const TString& binary)
{
    std::vector<TError> accumulatedErrors;

    auto test = [&] (const char* path) {
        if (access(path, R_OK | X_OK) == 0) {
            return true;
        } else {
            auto error = TError("No capabilities to run %Qlv", path) << TError::FromSystem();
            accumulatedErrors.push_back(std::move(error));
            return false;
        }
    };

    auto done = [&] () {
        auto error = TError(
            EProcessErrorCode::CannotResolveBinary,
            "Cannot resolve binary %Qlv",
            binary);
        error.InnerErrors().swap(accumulatedErrors);
        return error;
    };

    if (test(binary.c_str())) {
        return binary;
    }

    // If this is an absolute path, stop here.
    if (binary.empty() || binary[0] == '/') {
        return done();
    }

    // XXX(sandello): Sometimes we drop PATH from environment when spawning isolated processes.
    // In this case, try to locate somewhere nearby.
    {
        auto probe = TString::Join(GetDirName(GetExecPath()), "/", binary);
        if (test(probe.c_str())) {
            return probe;
        }
    }

    std::array<char, MAX_PATH> buffer;

    auto envPathStr = GetEnv("PATH");
    TStringBuf envPath(envPathStr);
    TStringBuf envPathItem;

    while (envPath.NextTok(':', envPathItem)) {
        if (buffer.size() < 2 + envPathItem.size() + binary.size()) {
            continue;
        }

        size_t index = 0;
        std::copy(envPathItem.begin(), envPathItem.end(), buffer.begin() + index);
        index += envPathItem.size();
        buffer[index] = '/';
        index += 1;
        std::copy(binary.begin(), binary.end(), buffer.begin() + index);
        index += binary.size();
        buffer[index] = 0;

        if (test(buffer.data())) {
            return TString(buffer.data(), index);
        }
    }

    return done();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

#ifdef _unix_

bool TryKill(int pid, int signal)
{
    YCHECK(pid > 0);
    int result = ::kill(pid, signal);
    // Ignore ESRCH because process may have died just before TryKill.
    if (result < 0 && errno != ESRCH) {
        return false;
    }
    return true;
}

bool TryWaitid(idtype_t idtype, id_t id, siginfo_t *infop, int options)
{
    while (true) {
        if (infop != nullptr) {
            // See comment below.
            infop->si_pid = 0;
        }

        siginfo_t info;
        ::memset(&info, 0, sizeof(info));
        auto res = HandleEintr(::waitid, idtype, id, infop != nullptr ? infop : &info, options);

        if (res == 0) {
            // According to man wait.
            // If WNOHANG was specified in options and there were
            // no children in a waitable state, then waitid() returns 0 immediately.
            // To distinguish this case from that where a child
            // was in a waitable state, zero out the si_pid field
            // before the call and check for a nonzero value in this field after
            // the call returns.
            if (infop && infop->si_pid == 0) {
                return false;
            }
            return true;
        }

        return false;
    }
}

void WaitidOrDie(idtype_t idtype, id_t id, siginfo_t *infop, int options)
{
    YCHECK(infop != nullptr);

    memset(infop, 0, sizeof(siginfo_t));

    bool isOK = TryWaitid(idtype, id, infop, options);

    if (!isOK) {
        YT_LOG_FATAL(TError::FromSystem(), "Waitid failed with options: %v", options);
    }

    YCHECK(infop->si_pid == id);
}

void Cleanup(int pid)
{
    YCHECK(pid > 0);

    YCHECK(TryKill(pid, 9));
    YCHECK(TryWaitid(P_PID, pid, nullptr, WEXITED));
}

bool TrySetSignalMask(const sigset_t* sigmask, sigset_t* oldSigmask)
{
    int error = pthread_sigmask(SIG_SETMASK, sigmask, oldSigmask);
    if (error != 0) {
        return false;
    }
    return true;
}

bool TryResetSignals()
{
    for (int sig = 1; sig < NSIG; ++sig) {
        // Ignore invalid signal errors.
        ::signal(sig, SIG_DFL);
    }
    return true;
}

#endif

} // namespace

////////////////////////////////////////////////////////////////////////////////

TProcessBase::TProcessBase(const TString& path)
     : Path_(path)
     , ProcessId_(InvalidProcessId)
{ }

void TProcessBase::AddArgument(TStringBuf arg)
{
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    Args_.push_back(Capture(arg));
}

void TProcessBase::AddEnvVar(TStringBuf var)
{
    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    Env_.push_back(Capture(var));
}

void TProcessBase::AddArguments(std::initializer_list<TStringBuf> args)
{
    for (auto arg : args) {
        AddArgument(arg);
    }
}

void TProcessBase::AddArguments(const std::vector<TString>& args)
{
    for (const auto& arg : args) {
        AddArgument(arg);
    }
}

void TProcessBase::SetWorkingDirectory(const TString& path)
{
    WorkingDirectory_ = path;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleProcess::TSimpleProcess(const TString& path, bool copyEnv, TDuration pollPeriod)
    // TString is guaranteed to be zero-terminated.
    // https://wiki.yandex-team.ru/Development/Poisk/arcadia/util/TStringAndTStringBuf#sobstvennosimvoly
    : TProcessBase(path)
    , PollPeriod_(pollPeriod)
    , PipeFactory_(3)
{
    AddArgument(path);

    if (copyEnv) {
        for (char** envIt = environ; *envIt; ++envIt) {
            Env_.push_back(Capture(*envIt));
        }
    }
}

void TSimpleProcess::AddDup2FileAction(int oldFD, int newFD)
{
    TSpawnAction action{
        std::bind(TryDup2, oldFD, newFD),
        Format("Error duplicating %v file descriptor to %v in child process", oldFD, newFD)
    };

    MaxSpawnActionFD_ = std::max(MaxSpawnActionFD_, newFD);
    SpawnActions_.push_back(action);
}

IConnectionReaderPtr TSimpleProcess::GetStdOutReader()
{
    auto& pipe = StdPipes_[STDOUT_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetWriteFD(), STDOUT_FILENO);
    return pipe.CreateAsyncReader();
}

IConnectionReaderPtr TSimpleProcess::GetStdErrReader()
{
    auto& pipe = StdPipes_[STDERR_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetWriteFD(), STDERR_FILENO);
    return pipe.CreateAsyncReader();
}

IConnectionWriterPtr TSimpleProcess::GetStdInWriter()
{
    auto& pipe = StdPipes_[STDIN_FILENO];
    pipe = PipeFactory_.Create();
    AddDup2FileAction(pipe.GetReadFD(), STDIN_FILENO);
    return pipe.CreateAsyncWriter();
}

TFuture<void> TProcessBase::Spawn()
{
    try {
        DoSpawn();
    } catch (const std::exception& ex) {
        FinishedPromise_.TrySet(ex);
    }
    return FinishedPromise_;
}

void TSimpleProcess::DoSpawn()
{
#ifdef _unix_
    auto finally = Finally([&] () {
        StdPipes_[STDIN_FILENO].CloseReadFD();
        StdPipes_[STDOUT_FILENO].CloseWriteFD();
        StdPipes_[STDERR_FILENO].CloseWriteFD();
        PipeFactory_.Clear();
    });

    YCHECK(ProcessId_ == InvalidProcessId && !Finished_);

    // Resolve binary path.
    std::vector<TError> innerErrors;
    for (int retryIndex = ResolveRetryCount; retryIndex >= 0; --retryIndex) {
        auto errorOrPath = ResolveBinaryPath(Path_);
        if (errorOrPath.IsOK()) {
            ResolvedPath_ = errorOrPath.Value();
            break;
        } else {
            innerErrors.push_back(errorOrPath);
        }

        if (retryIndex == 0) {
            auto error = TError("Failed to resolve binary path %v", Path_);
            error.InnerErrors() = innerErrors;
            error.ThrowOnError();
        } else {
            TDelayedExecutor::WaitForDuration(ResolveRetryTimeout);
        }
    }

    // Make sure no spawn action closes Pipe_.WriteFD
    TPipeFactory pipeFactory(MaxSpawnActionFD_ + 1);
    Pipe_ = pipeFactory.Create();
    pipeFactory.Clear();

    YT_LOG_DEBUG("Spawning new process (Path: %v, ErrorPipe: %v,  Arguments: %v, Environment: %v)",
        ResolvedPath_,
        Pipe_,
        Args_,
        Env_);

    Env_.push_back(nullptr);
    Args_.push_back(nullptr);

    // Block all signals around vfork; see http://ewontfix.com/7/

    // As the child may run in the same address space as the parent until
    // the actual execve() system call, any (custom) signal handlers that
    // the parent has might alter parent's memory if invoked in the child,
    // with undefined results.  So we block all signals in the parent before
    // vfork(), which will cause them to be blocked in the child as well (we
    // rely on the fact that Linux, just like all sane implementations, only
    // clones the calling thread).  Then, in the child, we reset all signals
    // to their default dispositions (while still blocked), and unblock them
    // (so the exec()ed process inherits the parent's signal mask)

    sigset_t allBlocked;
    sigfillset(&allBlocked);
    sigset_t oldSignals;

    if (!TrySetSignalMask(&allBlocked, &oldSignals)) {
        THROW_ERROR_EXCEPTION("Failed to block all signals")
            << TError::FromSystem();
    }

    SpawnActions_.push_back(TSpawnAction{
        TryResetSignals,
        "Error resetting signals to default disposition in child process: signal failed"
    });

    SpawnActions_.push_back(TSpawnAction{
        std::bind(TrySetSignalMask, &oldSignals, nullptr),
        "Error unblocking signals in child process: pthread_sigmask failed"
    });

    if (!WorkingDirectory_.empty()) {
        SpawnActions_.push_back(TSpawnAction{
            [&] () {
                NFs::SetCurrentWorkingDirectory(WorkingDirectory_);
                return true;
            },
            "Error changing working directory"
        });
    }

    SpawnActions_.push_back(TSpawnAction{
        [=] () {
            for (int retryIndex = 0; retryIndex < ExecveRetryCount; ++retryIndex) {
                // Execve may fail, if called binary is being updated, e.g. during yandex-yt package update.
                // So we'd better retry several times.
                // For example see YT-6352.
                TryExecve(ResolvedPath_.c_str(), Args_.data(), Env_.data());
                if (retryIndex < ExecveRetryCount - 1) {
                    Sleep(ExecveRetryTimeout);
                }
            }
            // If we are still here, return failure.
            return false;
        },
        "Error starting child process: execve failed"
    });

    SpawnChild();

    // This should not fail ever.
    YCHECK(TrySetSignalMask(&oldSignals, nullptr));

    Pipe_.CloseWriteFD();

    ValidateSpawnResult();

    AsyncWaitExecutor_ = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TSimpleProcess::AsyncPeriodicTryWait, MakeStrong(this)),
        PollPeriod_);

    AsyncWaitExecutor_->Start();
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::SpawnChild()
{
    // NB: fork() will cause data corruption when run concurrently with
    // Disk IO on O_DIRECT file descriptor. Seems like vfork don't suffer from the same issue.

#ifdef _unix_
    int pid = vfork();

    if (pid < 0) {
        THROW_ERROR_EXCEPTION("Error starting child process: vfork failed")
            << TErrorAttribute("path", ResolvedPath_)
            << TError::FromSystem();
    }

    if (pid == 0) {
        try {
            Child();
        } catch (...) {
            Y_UNREACHABLE();
        }
    }

    ProcessId_ = pid;
    Started_ = true;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::ValidateSpawnResult()
{
#ifdef _unix_
    int data[2];
    ssize_t res;
    res = HandleEintr(::read, Pipe_.GetReadFD(), &data, sizeof(data));
    Pipe_.CloseReadFD();

    if (res == 0) {
        // Child successfully spawned or was killed by a signal.
        // But there is no way to distinguish between these two cases:
        // * child killed by signal before exec
        // * child killed by signal after exec
        // So we treat kill-before-exec the same way as kill-after-exec.
        YT_LOG_DEBUG("Child process spawned successfully (Pid: %v)", ProcessId_);
        return;
    }

    YCHECK(res == sizeof(data));
    Finished_ = true;

    Cleanup(ProcessId_);
    ProcessId_ = InvalidProcessId;

    int actionIndex = data[0];
    int errorCode = data[1];

    YCHECK(0 <= actionIndex && actionIndex < SpawnActions_.size());
    const auto& action = SpawnActions_[actionIndex];
    THROW_ERROR_EXCEPTION("%v", action.ErrorMessage)
        << TError::FromSystem(errorCode);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

#ifdef _unix_
void TSimpleProcess::AsyncPeriodicTryWait()
{
    siginfo_t processInfo;
    memset(&processInfo, 0, sizeof(siginfo_t));

    // Note WNOWAIT flag.
    // This call just waits for a process to be finished but does not clear zombie flag.

    if (!TryWaitid(P_PID, ProcessId_, &processInfo, WEXITED | WNOWAIT | WNOHANG) ||
        processInfo.si_pid != ProcessId_)
    {
        return;
    }

    AsyncWaitExecutor_->Stop();
    AsyncWaitExecutor_ = nullptr;

    // This call just should return immediately
    // because we have already waited for this process with WNOHANG
    WaitidOrDie(P_PID, ProcessId_, &processInfo, WEXITED | WNOHANG);

    Finished_ = true;
    auto error = ProcessInfoToError(processInfo);
    YT_LOG_DEBUG("Process finished (Pid: %v, Error: %v)", ProcessId_, error);

    FinishedPromise_.Set(error);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSimpleProcess::Kill(int signal)
{
#ifdef _unix_
    if (!Started_) {
        THROW_ERROR_EXCEPTION("Process is not started yet");
    }

    if (Finished_) {
        return;
    }

    YT_LOG_DEBUG("Killing child process (Pid: %v)", ProcessId_);

    auto result = TryKill(ProcessId_, signal);
    if (!result) {
        THROW_ERROR_EXCEPTION("Failed to kill child process %v", ProcessId_)
            << TError::FromSystem();
    }
    return;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

TString TProcessBase::GetPath() const
{
    return Path_;
}

int TProcessBase::GetProcessId() const
{
    return ProcessId_;
}

bool TProcessBase::IsStarted() const
{
    return Started_;
}

bool TProcessBase::IsFinished() const
{
    return Finished_;
}

TString TProcessBase::GetCommandLine() const
{
    TStringBuilder builder;
    builder.AppendString(Path_);

    bool first = true;
    for (const auto& arg_ : Args_) {
        TStringBuf arg(arg_);
        if (first) {
            first = false;
        } else {
            if (arg) {
                builder.AppendChar(' ');
                bool needQuote = false;
                for (size_t i = 0; i < arg.length(); ++i) {
                    if (!IsAsciiAlnum(arg[i]) &&
                        arg[i] != '-' && arg[i] != '_' && arg[i] != '=' && arg[i] != '/')
                    {
                        needQuote = true;
                        break;
                    }
                }
                if (needQuote) {
                    builder.AppendChar('"');
                    TStringBuf left, right;
                    while (arg.TrySplit('"', left, right)) {
                        builder.AppendString(left);
                        builder.AppendString("\\\"");
                        arg = right;
                    }
                    builder.AppendString(arg);
                    builder.AppendChar('"');
                } else {
                    builder.AppendString(arg);
                }
            }
        }
    }

    return builder.Flush();
}

const char* TProcessBase::Capture(TStringBuf arg)
{
    StringHolders_.push_back(TString(arg));
    return StringHolders_.back().c_str();
}

void TSimpleProcess::Child()
{
#ifdef _unix_
    for (int actionIndex = 0; actionIndex < SpawnActions_.size(); ++actionIndex) {
        auto& action = SpawnActions_[actionIndex];
        if (!action.Callback()) {
            // Report error through the pipe.
            int data[] = {
                actionIndex,
                errno
            };

            // According to pipe(7) write of small buffer is atomic.
            ssize_t size = HandleEintr(::write, Pipe_.GetWriteFD(), &data, sizeof(data));
            YCHECK(size == sizeof(data));
            _exit(1);
        }
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
