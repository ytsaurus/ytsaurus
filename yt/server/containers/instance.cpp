#include "instance.h"

#ifdef _linux_

#include "porto_executor.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>

#include <yt/contrib/portoapi/libporto.hpp>

#include <initializer_list>

namespace NYT {
namespace NContainers {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// Porto passes command string to wordexp, where quota (') symbol
// is delimiter. So we must replace it with concatenation ('"'"').
static TString EscapeForWordexp(const char* in)
{
    TString buffer;
    while (*in) {
        if (*in == '\'') {
            buffer.append(R"('"'"')");
        } else {
            buffer.append(*in);
        }
        in++;
    }
    return buffer;
}

using TPortoStatRule = std::pair<TString, TCallback<i64(const TString& input)>>;

static i64 Extract(const TString& input, const TString& pattern, const TString& terminator = "\n")
{
    auto start = input.find(pattern) + pattern.length();
    auto end = input.find(terminator, start);
    return std::stol(input.substr(start, (end == input.npos) ? end : end - start));
}

static i64 ExtractSum(const TString& input, const TString& pattern, const TString& delimiter, const TString& terminator = "\n")
{
    i64 sum = 0;
    TString::size_type pos = 0;
    while (pos < input.length()) {
        pos = input.find(pattern, pos);
        if (pos == input.npos) {
            break;
        }
        pos += pattern.length();

        pos = input.find(delimiter, pos);
        if (pos == input.npos) {
            break;
        }

        pos++;
        auto end = input.find(terminator, pos);
        sum += std::stol(input.substr(pos, (end == input.npos) ? end : end - pos));
    }
    return sum;
}

////////////////////////////////////////////////////////////////////////////////

class TPortoInstance
    : public IInstance
{
public:
    static IInstancePtr Create(const TString& name, IPortoExecutorPtr executor)
    {
        auto error = WaitFor(executor->CreateContainer(name));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to create container");
        return New<TPortoInstance>(name, executor);
    }

    static IInstancePtr GetSelf(IPortoExecutorPtr executor)
    {
        return New<TPortoInstance>("self", executor);
    }

    ~TPortoInstance()
    {
        // We can't wait here, but even if this request fails
        // it is not a big issue - porto has its own GC.
        if (!Destroyed_) {
            Executor_->DestroyContainer(Name_);
        }
    }

    virtual void SetStdIn(const TString& inputPath) override
    {
        SetProperty("stdin_path", inputPath);
    }

    virtual void SetStdOut(const TString& outPath) override
    {
        SetProperty("stdout_path", outPath);
    }

    virtual void SetStdErr(const TString& errorPath) override
    {
        SetProperty("stderr_path", errorPath);
    }

    virtual void SetCwd(const TString& cwd) override
    {
        SetProperty("cwd", cwd);
    }

    virtual void Kill(int signal) override
    {
        auto error = WaitFor(Executor_->Kill(Name_, signal));
        // Killing already finished process is not an error.
        if (error.FindMatching(EContainerErrorCode::InvalidState)) {
            return;
        }
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to send signal to porto instance")
                << TErrorAttribute("signal", signal)
                << TErrorAttribute("container", Name_)
                << error;
        }
    }

    virtual void Destroy() override
    {
        WaitFor(Executor_->DestroyContainer(Name_))
            .ThrowOnError();
        Destroyed_ = true;
    }

    virtual TUsage GetResourceUsage(const std::vector<EStatField>& fields) const override
    {
        std::vector<TString> properties;
        TUsage result;
        try {
            for (auto field : fields) {
                const auto& rules = StatRules_.at(field);
                properties.push_back(rules.first);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Unknown resource field requested");
            THROW_ERROR_EXCEPTION("Unknown resource field requested")
                << TErrorAttribute("container", Name_)
                << ex;
        }

        auto response = WaitFor(Executor_->GetProperties(Name_, properties))
            .ValueOrThrow();

        for (auto field : fields) {
            const auto& rules = StatRules_.at(field);
            TErrorOr<ui64>& record = result[field];
            try {
                auto data = response.at(rules.first)
                    .ValueOrThrow();
                record = rules.second(data);
            } catch (const std::exception& ex) {
                record = TError("Unable to get %Qv from porto", rules.first)
                    << TErrorAttribute("container", Name_)
                    << ex;
            }
        }
        return result;
    }

    virtual void SetCpuLimit(double cores) override
    {
        SetProperty("cpu_limit", ToString(cores) + "c");
    }

    virtual void SetCpuShare(double cores) override
    {
        SetProperty("cpu_guarantee", ToString(cores) + "c");
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        SetProperty("io_ops_limit", ToString(operations));
    }

    virtual TString GetName() const override
    {
        return Name_;
    }

    virtual pid_t GetPid() const override
    {
        auto pid = WaitFor(Executor_->GetProperties(Name_, std::vector<TString>{"root_pid"}))
            .ValueOrThrow();
        return std::stoi(pid.at("root_pid")
            .ValueOrThrow());
    }

    virtual TFuture<int> Exec(
        const std::vector<const char*>& argv,
        const std::vector<const char*>& env) override
    {
        TString command;

        for (auto arg : argv) {
            command += TString("'") + EscapeForWordexp(arg) + TString("'");
            command += " ";
        }

        LOG_DEBUG("Executing porto container (Command: %v)", command);

        SetProperty("controllers", "freezer;memory;cpu;cpuacct;net_cls;blkio;devices");
        SetProperty("command", command);
        SetProperty("isolate", "true");
        SetProperty("enable_porto", "true");
        SetProperty("porto_namespace", Name_ + "/");

        for (auto arg : env) {
            SetProperty("env", TString(arg) + ";");
        }

        // Wait for all pending actions - do not start real execution if
        // preparation has failed
        WaitForActions().ThrowOnError();
        TFuture<void> startAction = Executor_->Start(Name_);
        // Wait for starting process - here we get error if exec has failed
        // i.e. no such file, execution bit, etc
        // In theory it is not necessarily to wait here, but in this case
        // error handling will be more difficult.
        auto error = WaitFor(startAction);
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to start container");

        return Executor_->AsyncPoll(Name_);
    }

    virtual void MountTmpfs(const TString& path, const size_t size, const TString& user) override
    {
        std::map<TString, TString> config;
        config["backend"] = "tmpfs";
        config["user"] = user;
        config["space_limit"] = ToString(size);
        auto mountId = WaitFor(Executor_->CreateVolume(path, config))
            .ValueOrThrow();

        std::vector<TFuture<void>> mountActions;
        mountActions.push_back(Executor_->LinkVolume(mountId.Path, Name_));
        mountActions.push_back(Executor_->UnlinkVolume(mountId.Path, ""));
        WaitFor(Combine(mountActions)).ThrowOnError();
    }

    virtual void Umount(const TString& path) override
    {
        WaitFor(Executor_->UnlinkVolume(path, Name_)).ThrowOnError();
    }

    virtual std::vector<NFS::TMountPoint> ListVolumes() const override
    {
        std::vector<NFS::TMountPoint> result;
        auto volumes = WaitFor(Executor_->ListVolumes()).ValueOrThrow();
        // O(n^2) but only if all mountpoints is mounted to each container and namespace is disabled.
        for (const auto& volume : volumes) {
            for (auto container : volume.Containers) {
                if (container == Name_) {
                    result.push_back({TString(), volume.Path});
                }
            }
        }
        return result;
    }

private:
    const TString Name_;
    mutable IPortoExecutorPtr Executor_;
    std::vector<TFuture<void>> Actions_;
    static const std::map<EStatField, TPortoStatRule> StatRules_;
    const NLogging::TLogger Logger;
    bool Destroyed_ = false;

    TPortoInstance(
        const TString& name,
        IPortoExecutorPtr executor)
        : Name_(name)
        , Executor_(executor)
        , Logger(NLogging::TLogger(ContainersLogger)
            .AddTag("Container: %v", Name_))
    { }

    void SetProperty(const TString& key, const TString& value)
    {
        Actions_.push_back(Executor_->SetProperty(Name_, key, value));
    }

    TError WaitForActions()
    {
        auto error = WaitFor(Combine(Actions_));
        Actions_.clear();
        return error;
    }

    DECLARE_NEW_FRIEND();
};

const std::map<EStatField, TPortoStatRule> TPortoInstance::StatRules_ = {
    { EStatField::CpuUsageUser,    { "cpu_usage",
        BIND([](const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::CpuUsageSystem,  { "cpu_usage_system",
        BIND([](const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::CpuStolenTime,   { "cpu_wait_time",
        BIND([](const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::Rss,             { "memory.stat",
        BIND([](const TString& in) { return Extract(in, "rss");                } ) } },
    { EStatField::MappedFiles,     { "memory.stat",
        BIND([](const TString& in) { return Extract(in, "mapped_file");        } ) } },
    { EStatField::IOOperations,    { "io_ops",
        BIND([](const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::IOReadByte,      { "io_read",
        BIND([](const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::IOWriteByte,      { "io_write",
        BIND([](const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::MaxMemoryUsage,  { "memory.max_usage_in_bytes",
        BIND([](const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::MajorFaults,     { "major_faults",
        BIND([](const TString& in) { return std::stol(in);                     } ) } }
};

////////////////////////////////////////////////////////////////////////////////

IInstancePtr CreatePortoInstance(const TString& name, IPortoExecutorPtr executor)
{
    return TPortoInstance::Create(name, executor);
}

IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor)
{
    return TPortoInstance::GetSelf(executor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT

#else

namespace NYT {
namespace NContainers {

////////////////////////////////////////////////////////////////////////////////

IInstancePtr CreatePortoInstance(const TString& /*name*/, IPortoExecutorPtr /*executor*/)
{
    Y_UNIMPLEMENTED();
}

IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr /*executor*/)
{
    Y_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT

#endif
