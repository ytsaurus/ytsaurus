#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_cgroup_mixin.h>
#include <yt/ytlib/program/program_tool_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/server/job_proxy/job_satellite.h>
#include <yt/server/job_proxy/job_satellite_connection.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/pipes/pipe.h>
#include <yt/core/misc/proc.h>

#include <sys/ioctl.h>

#ifdef _unix_
    #include <sys/resource.h>
#endif

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TExecProgram
    : public TProgram
    , public TProgramConfigMixin<NJobProxy::TJobSatelliteConnectionConfig>
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
            .AddLongOption("pty", "attach to the pseudoterminal")
            .StoreResult(&Pty_)
            .RequiredArgument("PTY");
        Opts_
            .AddLongOption("enable-core-dump", "whether to adjust resource limits to allow core dumps")
            .SetFlag(&EnableCoreDump_)
            .NoArgument();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        if (HandleConfigOptions()) {
            return;
        }

        ConfigureUids();
        ConfigureCrashHandler();

        if (Pty_ == -1) {
            NJobProxy::RunJobSatellite(GetConfig(), Uid_, Environment_, JobId_);
        }
        TThread::CurrentThreadSetName("ExecMain");

        // Don't start any other singleton or parse config in executor mode.
        // Explicitly shut down log manager to ensure it doesn't spoil dup-ed descriptors.
        NLogging::TLogManager::StaticShutdown();

        if (HandleCgroupOptions()) {
            return;
        }

        TError executorError;

        static const int PipePermissions = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

        try {
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

                    SetPermissions(streamFd, PipePermissions);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to prepare named pipe")
                        << TErrorAttribute("path", path)
                        << TErrorAttribute("fd", streamFd)
                        << ex;
                }
            }

            struct rlimit rlimit = {
                EnableCoreDump_ ? RLIM_INFINITY : 0,
                EnableCoreDump_ ? RLIM_INFINITY : 0
            };

            auto rv = setrlimit(RLIMIT_CORE, &rlimit);
            if (rv) {
                THROW_ERROR_EXCEPTION("Failed to configure core dump limits")
                    << TError::FromSystem();
            }
        } catch (const std::exception& ex) {
            executorError = ex;
        }

        if (!executorError.IsOK()) {
            Exit(3);
            return;
        }

        if (Pty_ != -1) {
            CloseAllDescriptors({Pty_});
            if (setsid() == -1) {
                THROW_ERROR_EXCEPTION("Failed to create a new session")
                    << TError::FromSystem();
            }
            if (::ioctl(Pty_, TIOCSCTTY, 1) == -1) {
                THROW_ERROR_EXCEPTION("Failed to set controlling pseudoterminal")
                    << TError::FromSystem();
            }
            SafeDup2(Pty_, 0);
            SafeDup2(Pty_, 1);
            SafeDup2(Pty_, 2);
            if (Pty_ > 2) {
                SafeClose(Pty_);
            }
        }

        if (Uid_ > 0) {
            SetUid(Uid_);
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
        if (Pty_ == -1) {
            try {
                NJobProxy::NotifyExecutorPrepared(GetConfig());
            } catch (const std::exception& ex) {
                fprintf(stderr, "Unable to notify job proxy\n%s", ex.what());
                Y_UNREACHABLE();
            }
        }

        TryExecve(
            "/bin/bash",
            args.data(),
            env.data());

        Exit(3);
        return;
    }

private:
    TString Command_;
    std::vector<NPipes::TNamedPipeConfig> Pipes_;
    std::vector<TString> Environment_;
    TString JobId_;
    int Uid_ = -1;
    int Pty_ = -1;
    bool EnableCoreDump_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TExecProgram().Run(argc, argv);
}

