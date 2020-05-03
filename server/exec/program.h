#include "job_satellite.h"

#include <yt/server/lib/job_satellite_connection/job_satellite_connection.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_cgroup_mixin.h>
#include <yt/ytlib/program/program_tool_mixin.h>
#include <yt/ytlib/program/helpers.h>

#include <yt/core/logging/formatter.h>
#include <yt/core/logging/log_manager.h>

#include <yt/library/process/pipe.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/fs.h>

#include <sys/ioctl.h>

#ifdef _unix_
    #include <sys/resource.h>
#endif

namespace NYT::NExec {

using namespace NJobSatelliteConnection;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TExecProgram
    : public TProgram
    , public TProgramConfigMixin<TJobSatelliteConnectionConfig>
    , public TProgramCgroupMixin
{
public:
    TExecProgram()
        : TProgramConfigMixin(Opts_, false)
        , TProgramCgroupMixin(Opts_)
    {
        Opts_
            .AddLongOption("command", "command to execute")
            .StoreResult(&Command_)
            .RequiredArgument("COMMAND")
            .Optional();
        Opts_
            .AddLongOption("pipe", "configure a data pipe (could be used multiple times)")
            .Handler1T<TString>([&] (const TString& arg) {
                try {
                    auto config = ConvertTo<NPipes::TNamedPipeConfig>(TYsonString(arg));
                    Pipes_.push_back(std::move(config));
                } catch (const std::exception& ex) {
                    throw TProgramException(Format("Bad pipe config: %v", ex.what()));
                }
            })
            .RequiredArgument("PIPE_CONFIG")
            .Optional();
        Opts_
            .AddLongOption("job-id", "job id")
            .StoreResult(&JobId_)
            .RequiredArgument("ID")
            .Optional();
        Opts_
            .AddLongOption("env", "set up environment for a child process (could be used multiple times)")
            .Handler1T<TString>([&] (const TString& arg) {
                if (arg.find('=') == TString::npos) {
                    throw TProgramException(Format("Bad environment variable: missing '=' in %Qv", arg));
                }
                Environment_.push_back(arg);
            })
            .RequiredArgument("KEY=VALUE")
            .Required();
        Opts_
            .AddLongOption("uid", "user to impersonate before spawning a child process")
            .StoreResult(&Uid_)
            .RequiredArgument("UID");
        Opts_
            .AddLongOption("enable-core-dump", "whether to adjust resource limits to allow core dumps")
            .SetFlag(&EnableCoreDump_)
            .NoArgument();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        ExecutorStderr_ = TFile{"../executor_stderr", EOpenModeFlag::WrOnly | EOpenModeFlag::ForAppend | EOpenModeFlag::OpenAlways};

        if (HandleConfigOptions()) {
            return;
        }

        // Make RSS predictable.
        NYTAlloc::SetEnableEagerMemoryRelease(true);

        ConfigureUids();
        ConfigureCrashHandler();

        RunJobSatellite(GetConfig(), Uid_, JobId_);

        TThread::SetCurrentThreadName("ExecMain");

        // Don't start any other singleton or parse config in executor mode.
        // Explicitly shut down log manager to ensure it doesn't spoil dup-ed descriptors.
        NLogging::TLogManager::StaticShutdown();

        if (HandleCgroupOptions()) {
            return;
        }

        TError executorError;

        static const int PipePermissions = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

        try {
            struct rlimit rlimit = {
                EnableCoreDump_ ? RLIM_INFINITY : 0,
                EnableCoreDump_ ? RLIM_INFINITY : 0
            };

            auto rv = setrlimit(RLIMIT_CORE, &rlimit);
            if (rv) {
                THROW_ERROR_EXCEPTION("Failed to configure core dump limits")
                    << TError::FromSystem();
            }

            for (const auto& pipe : Pipes_) {
                const int streamFd = pipe.FD;
                const auto& path = pipe.Path;

                try {
                    // Behaviour of named pipe:
                    // reader blocks on open if no writer and O_NONBLOCK is not set,
                    // writer blocks on open if no reader and O_NONBLOCK is not set.
                    const int flags = (pipe.Write ? O_WRONLY : O_RDONLY);
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

                    NFS::SetPermissions(streamFd, PipePermissions);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to prepare named pipe")
                        << TErrorAttribute("path", path)
                        << TErrorAttribute("fd", streamFd)
                        << ex;
                }
            }
        } catch (const std::exception& ex) {
            executorError = ex;
        }

        if (!executorError.IsOK()) {
            LogToStderr(Format("Failed to prepare pipes, unexpected executor error\n%v", executorError));
            Exit(4);
        }

        std::vector<char*> env;
        for (auto environment : Environment_) {
            env.push_back(const_cast<char*>(environment.c_str()));
        }
        env.push_back(const_cast<char*>("SHELL=/bin/bash"));
        env.push_back(nullptr);

        std::vector<const char*> args;
        args.push_back("/bin/bash");

        TString command;
        if (!Command_.empty()) {
            // :; is added avoid fork/exec (one-shot) optimization.
            command = ":; " + Command_;
            args.push_back("-c");
            args.push_back(command.c_str());
        }
        args.push_back(nullptr);

        // We are ready to execute user code, send signal to JobProxy.
        try {
            NotifyExecutorPrepared(GetConfig());
        } catch (const std::exception& ex) {
            LogToStderr(Format("Unable to notify job proxy\n%v", ex.what()));
            Exit(5);
        }

        if (Uid_ > 0) {
            SetUid(Uid_);
        }

        TryExecve(
            "/bin/bash",
            args.data(),
            env.data());

        LogToStderr(Format("execve failed: %v", TError::FromSystem()));
        Exit(6);
    }

    virtual void OnError(const TString& message) const noexcept override
    {
        LogToStderr(message);
    }

private:
    void LogToStderr(const TString& message) const
    {
        auto logRecord = Format("%v (JobId: %v)", message, JobId_);

        if (!ExecutorStderr_.IsOpen()) {
            Cerr << logRecord << Endl;
            return;
        }

        ExecutorStderr_.Write(logRecord.data(), logRecord.size());
        ExecutorStderr_.Flush();
    }

    mutable TFile ExecutorStderr_;
    TString Command_;
    std::vector<NPipes::TNamedPipeConfig> Pipes_;
    std::vector<TString> Environment_;
    TString JobId_;
    int Uid_ = -1;
    bool EnableCoreDump_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
