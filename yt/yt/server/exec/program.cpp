#include "program.h"

#include "user_job_synchronizer.h"

#include <yt/yt/library/process/pipe.h>

#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/system/thread.h>

#include <sys/ioctl.h>

#ifdef _unix_
    #include <sys/resource.h>
    #include <unistd.h>
#endif

namespace NYT::NExec {

////////////////////////////////////////////////////////////////////////////////

TExecProgram::TExecProgram()
    : TProgramConfigMixin(Opts_, false)
{ }

void TExecProgram::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    Cerr << "HERE GetConfig" << Endl;
    auto config = GetConfig();

    Cerr << "HERE JobId" << Endl;
    JobId_ = config->JobId;

    Cerr << "HERE 0" << Endl;
    if (HandleConfigOptions()) {
        return;
    }
    Cerr << "HERE 1" << Endl;

    ConfigureUids();

    Cerr << "HERE 2" << Endl;
    ConfigureCrashHandler();
    Cerr << "HERE 3" << Endl;

    TThread::SetCurrentThreadName("ExecMain");

    Cerr << "HERE 4 log manager shutdown" << Endl;
    // Don't start any other singleton or parse config in executor mode.
    // Explicitly shut down log manager to ensure it doesn't spoil dup-ed descriptors.
    NLogging::TLogManager::Get()->Shutdown();

    Cerr << "HERE 5 SetUid" << Endl;
    if (config->Uid > 0) {
        SetUid(config->Uid);
    }

    TError executorError;

    try {
        Cerr << "HERE 6 enable core dump: " << config->EnableCoreDump << Endl;
        auto enableCoreDump = config->EnableCoreDump;
        struct rlimit rlimit = {
            enableCoreDump ? RLIM_INFINITY : 0,
            enableCoreDump ? RLIM_INFINITY : 0
        };
        Cerr << "HERE 7 setrlimit" << Endl;

        auto rv = setrlimit(RLIMIT_CORE, &rlimit);
        Cerr << "HERE 8 setrlimit finished: '" << rv << "'" << Endl;
        if (rv) {
            Cerr << "HERE 9 throw error" << Endl;
            THROW_ERROR_EXCEPTION("Failed to configure core dump limits")
                << TError::FromSystem();
        }
        Cerr << "HERE 10" << Endl;

        for (const auto& pipe : config->Pipes) {
            auto streamFd = pipe->FD;
            const auto& path = pipe->Path;

            try {
                // Behaviour of named pipe:
                // reader blocks on open if no writer and O_NONBLOCK is not set,
                // writer blocks on open if no reader and O_NONBLOCK is not set.
                auto flags = (pipe->Write ? O_WRONLY : O_RDONLY);
                auto fd = HandleEintr(::open, path.c_str(), flags);
                if (fd == -1) {
                    THROW_ERROR_EXCEPTION("Failed to open named pipe")
                        << TErrorAttribute("path", path)
                        << TError::FromSystem();
                }

                if (streamFd != fd) {
                    SafeDup2(fd, streamFd);
                    SafeClose(fd, false);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to prepare named pipe")
                    << TErrorAttribute("path", path)
                    << TErrorAttribute("fd", streamFd)
                    << ex;
            }
        }
    } catch (const std::exception& ex) {
        executorError = ex;
        Cerr << "HERE 11 has executor error" << Endl;
    }

    try {
        Cerr << "HERE 12" << Endl;
        OpenExecutorStderr();
        Cerr << "HERE 13" << Endl;
    } catch (const std::exception& ex) {
        Exit(ToUnderlying(EProgramExitCode::ExecutorStderrOpenError));
    }

    if (!executorError.IsOK()) {
        LogToStderr(Format("Failed to prepare pipes, unexpected executor error\n%v\n", executorError));
        Cerr << "HERE 14 exit with 102" << Endl;
        Exit(ToUnderlying(EProgramExitCode::ExecutorError));
    }

    // NB: intentionally open executor_stderr after processing pipes to avoid fd clashes.
    try {
        if (ExecutorStderr_.GetHandle() == STDOUT_FILENO) {
            auto newFile = ExecutorStderr_.Duplicate();
            ExecutorStderr_.Close();
            ExecutorStderr_ = newFile;
        }
    } catch (const std::exception& ex) {
        Exit(ToUnderlying(EProgramExitCode::ExecutorStderrDuplicateError));
    }

    std::vector<const char*> env;
    for (const auto& environment : config->Environment) {
        env.push_back(environment.c_str());
    }
    env.push_back("SHELL=/bin/bash");
    env.push_back(nullptr);

    std::vector<const char*> args;
    args.push_back("/bin/bash");

    TString command;
    if (!config->Command.empty()) {
        // :; is added avoid fork/exec (one-shot) optimization.
        command = ":; " + config->Command;
        args.push_back("-c");
        args.push_back(command.c_str());
    }
    args.push_back(nullptr);

    // We are ready to execute user code, send signal to JobProxy.
    try {
        auto jobProxyControl = CreateUserJobSynchronizerClient(config->UserJobSynchronizerConnectionConfig);
        jobProxyControl->NotifyExecutorPrepared();
    } catch (const std::exception& ex) {
        LogToStderr(Format("Unable to notify job proxy\n%v", ex.what()));
        Exit(ToUnderlying(EProgramExitCode::JobProxyNotificationError));
    }

    TryExecve(
        "/bin/bash",
        args.data(),
        env.data());

    LogToStderr(Format("execve failed: %v", TError::FromSystem()));
    Exit(ToUnderlying(EProgramExitCode::ExecveError));
}

void TExecProgram::OnError(const TString& message) noexcept
{
    LogToStderr(message);
}

void TExecProgram::LogToStderr(const TString& message)
{
    auto logRecord = Format("%v (JobId: %v)", message, JobId_);

    if (!ExecutorStderr_.IsOpen()) {
        Cerr << logRecord << Endl;
        return;
    }

    ExecutorStderr_.Write(logRecord.data(), logRecord.size());
    ExecutorStderr_.Flush();
}

void TExecProgram::OpenExecutorStderr()
{
    ExecutorStderr_ = TFile(
        GetConfig()->StderrPath,
        EOpenModeFlag::WrOnly | EOpenModeFlag::ForAppend | EOpenModeFlag::OpenAlways);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
