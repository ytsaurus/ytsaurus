#ifdef __linux__

#include "instance.h"

#include "porto_executor.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/fs.h>

#include <infra/porto/api/libporto.hpp>

#include <util/string/cast.h>

#include <initializer_list>
#include <string>

namespace NYT::NContainers {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Porto passes command string to wordexp, where quota (') symbol
// is delimiter. So we must replace it with concatenation ('"'"').
TString EscapeForWordexp(const char* in)
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

i64 Extract(const TString& input, const TString& pattern, const TString& terminator = "\n")
{
    auto start = input.find(pattern) + pattern.length();
    auto end = input.find(terminator, start);
    return std::stol(input.substr(start, (end == input.npos) ? end : end - start));
}

i64 ExtractSum(const TString& input, const TString& pattern, const TString& delimiter, const TString& terminator = "\n")
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

using TPortoStatRule = std::pair<TString, TCallback<i64(const TString& input)>>;

const THashMap<EStatField, TPortoStatRule> PortoStatRules = {
    { EStatField::CpuUsageUser,     { "cpu_usage",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::CpuUsageSystem,   { "cpu_usage_system",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::CpuWait,          { "cpu_wait",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::CpuThrottled,     { "cpu_throttled",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::Rss,              { "memory.stat",
        BIND([] (const TString& in) { return Extract(in, "total_rss");          } ) } },
    { EStatField::MappedFiles,      { "memory.stat",
        BIND([] (const TString& in) { return Extract(in, "total_mapped_file");  } ) } },
    { EStatField::IOOperations,     { "io_ops",
        BIND([] (const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::IOReadByte,       { "io_read",
        BIND([] (const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::IOWriteByte,      { "io_write",
        BIND([] (const TString& in) { return ExtractSum(in, "sd", ":", ";");    } ) } },
    { EStatField::MaxMemoryUsage,   { "memory.max_usage_in_bytes",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::MajorFaults,      { "major_faults",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPortoInstance
    : public IInstance
{
public:
    static IInstancePtr Create(const TString& name, IPortoExecutorPtr executor, bool autoDestroy)
    {
        auto error = WaitFor(executor->CreateContainer(name));
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Unable to create container");
        return New<TPortoInstance>(name, executor, autoDestroy);
    }

    static IInstancePtr GetSelf(IPortoExecutorPtr executor)
    {
        return New<TPortoInstance>("self", executor, false);
    }

    static IInstancePtr GetInstance(IPortoExecutorPtr executor, const TString& name)
    {
        return New<TPortoInstance>(name, executor, false);
    }

    ~TPortoInstance()
    {
        // We can't wait here, but even if this request fails
        // it is not a big issue - Porto has its own GC.
        if (!Destroyed_ && AutoDestroy_) {
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

    virtual void SetCoreDumpHandler(const TString& handler) override
    {
        SetProperty("core_command", handler);
    }

    virtual void SetNet(const TString& net) override
    {
        SetProperty("net", net);
    }

    virtual void SetIP(const TString& ip) override
    {
        SetProperty("ip", ip);
    }

    virtual void SetHostName(const TString& hostName) override
    {
        SetProperty("hostname", hostName);
    }

    virtual void Kill(int signal) override
    {
        auto error = WaitFor(Executor_->KillContainer(Name_, signal));
        // Killing already finished process is not an error.
        if (error.FindMatching(EPortoErrorCode::InvalidState)) {
            return;
        }
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to send signal to Porto instance")
                << TErrorAttribute("signal", signal)
                << TErrorAttribute("container", Name_)
                << error;
        }
    }

    virtual void SetRoot(const TRootFS& rootFS) override
    {
        HasRoot_ = true;
        SetProperty("root", rootFS.RootPath);
        SetProperty("root_readonly", TString(FormatBool(rootFS.IsRootReadOnly)));

        TStringBuilder builder;
        for (const auto& bind : rootFS.Binds) {
            builder.AppendString(bind.SourcePath);
            builder.AppendString(" ");
            builder.AppendString(bind.TargetPath);
            builder.AppendString(" ");
            builder.AppendString(bind.IsReadOnly ? "ro" : "rw");
            builder.AppendString(" ; ");
        }

        SetProperty("bind", builder.Flush());
    }

    virtual void SetDevices(const std::vector<TDevice>& devices) override
    {
        TStringBuilder builder;
        for (const auto& device : devices) {
            builder.AppendString(device.DeviceName);
            builder.AppendString(" ");
            if (device.Enabled) {
                builder.AppendString("rw");
            } else {
                builder.AppendString("-");
            }
            builder.AppendString(" ; ");
        }

        if (NFS::Exists("/dev/kvm")) {
            builder.AppendString("/dev/kvm rw");
        }

        SetProperty("devices", builder.Flush());
    }

    virtual bool HasRoot() const override
    {
        return HasRoot_;
    }

    virtual void Destroy() override
    {
        WaitFor(Executor_->DestroyContainer(Name_))
            .ThrowOnError();
        Destroyed_ = true;
    }

    virtual void Stop() override
    {
        WaitFor(Executor_->StopContainer(Name_))
            .ThrowOnError();
    }

    virtual TString GetRoot() override
    {
        auto getRoot = [&] (TString name) {
            auto properties = WaitFor(Executor_->GetContainerProperties(
                name,
                std::vector<TString>{"root"}))
                .ValueOrThrow();

            return properties.at("root")
                .ValueOrThrow();
        };

        static const TString Prefix("/porto");

        TString root = "/";
        auto absoluteName = GetAbsoluteName();
        while (true) {
            YT_VERIFY(absoluteName.length() >= Prefix.length());
            if (absoluteName == Prefix) {
                return root;
            }

            root = getRoot(absoluteName);
            if (root != "/") {
                return root;
            }

            auto slashPosition = absoluteName.rfind('/');
            absoluteName = absoluteName.substr(0, slashPosition);
        }
    }

    virtual TResourceUsage GetResourceUsage(const std::vector<EStatField>& fields) const override
    {
        std::vector<TString> properties;
        properties.push_back("absolute_name");

        bool contextSwitchesRequested = false;
        for (auto field : fields) {
            if (auto it = PortoStatRules.find(field)) {
                const auto& rule = it->second;
                properties.push_back(rule.first);
            } else if (field == EStatField::ContextSwitches) {
                contextSwitchesRequested = true;
            } else {
                THROW_ERROR_EXCEPTION("Unknown resource field %Qlv requested", field)
                    << TErrorAttribute("container", Name_);
            }
        }

        auto propertyMap = WaitFor(Executor_->GetContainerProperties(Name_, properties))
            .ValueOrThrow();

        TResourceUsage result;
        
        for (auto field : fields) {
            auto ruleIt = PortoStatRules.find(field);
            if (ruleIt == PortoStatRules.end()) {
                continue;
            }

            const auto& [property, callback] = ruleIt->second;
            auto& record = result[field];
            if (auto responseIt = propertyMap.find(property); responseIt != propertyMap.end()) {
                const auto& valueOrError = responseIt->second;
                if (valueOrError.IsOK()) {
                    const auto& value = valueOrError.Value();
                    try {
                        record = callback(value);
                    } catch (const std::exception& ex) {
                        record = TError("Error parsing Porto property %Qlv", field)
                            << TErrorAttribute("container", Name_)
                            << TErrorAttribute("property_value", value)
                            << ex;
                    }
                } else {
                    record = TError("Error getting Porto property %Qlv", field)
                        << TErrorAttribute("container", Name_)
                        << valueOrError;
                }
             } else {
                record = TError("Missing property %Qlv in Porto response", field)
                    << TErrorAttribute("container", Name_);
            }
        }

        // We should maintain context switch information even if this field
        // is not requested since metrics of individual containers can go up and down.
        
        auto selfAbsoluteName = GetOrCrash(propertyMap, "absolute_name")
            .ValueOrThrow();

        auto subcontainers = WaitFor(Executor_->ListSubcontainers(selfAbsoluteName, true))
            .ValueOrThrow();

        auto metricMap = WaitFor(Executor_->GetContainerMetrics(subcontainers, "ctxsw"))
            .ValueOrThrow();

        {
            auto guard = Guard(ContextSwitchMapLock_);

            for (const auto& [container, newValue] : metricMap) {
                auto& prevValue = ContextSwitchMap_[container];
                TotalContextSwitches_ += std::max<i64>(0LL, newValue - prevValue);
                prevValue = newValue;
            }

            if (contextSwitchesRequested) {
                result[EStatField::ContextSwitches] = TotalContextSwitches_;
            }
        }
        
        return result;
    }

    virtual TResourceLimits GetResourceLimits() const override
    {
        std::vector<TString> properties;
        properties.push_back("memory_limit");
        properties.push_back("cpu_limit");

        auto responseOrError = WaitFor(Executor_->GetContainerProperties(Name_, properties));
        THROW_ERROR_EXCEPTION_IF_FAILED(responseOrError, "Failed to get Porto container resource limits");

        const auto& response = responseOrError.Value();
        const auto& memoryLimitRsp = response.at("memory_limit");

        THROW_ERROR_EXCEPTION_IF_FAILED(memoryLimitRsp, "Failed to get memory limit from Porto");

        i64 memoryLimit;

        if (!TryFromString<i64>(memoryLimitRsp.Value(), memoryLimit)) {
            THROW_ERROR_EXCEPTION("Failed to parse memory limit value from Porto")
                << TErrorAttribute("memory_limit", memoryLimitRsp.Value());
        }

        const auto& cpuLimitRsp = response.at("cpu_limit");

        THROW_ERROR_EXCEPTION_IF_FAILED(cpuLimitRsp, "Failed to get CPU limit from Porto");

        double cpuLimit;

        YT_VERIFY(cpuLimitRsp.Value().EndsWith('c'));
        auto cpuLimitValue = TStringBuf(cpuLimitRsp.Value().begin(), cpuLimitRsp.Value().size() - 1);
        if (!TryFromString<double>(cpuLimitValue, cpuLimit)) {
            THROW_ERROR_EXCEPTION("Failed to parse CPU limit value from orto")
                << TErrorAttribute("cpu_limit", cpuLimitRsp.Value());
        }

        return TResourceLimits{cpuLimit, memoryLimit};
    }

    virtual TResourceLimits GetResourceLimitsRecursive() const override
    {
        static const TString Prefix("/porto");

        auto resourceLimits = GetResourceLimits();

        auto absoluteName = GetAbsoluteName();

        auto slashPosition = absoluteName.rfind('/');
        auto parentName = absoluteName.substr(0, slashPosition);

        if (parentName != Prefix) {
            YT_VERIFY(parentName.length() > Prefix.length());
            auto parent = GetInstance(Executor_, parentName);
            auto parentLimits = parent->GetResourceLimitsRecursive();

            if (resourceLimits.Cpu == 0 || (parentLimits.Cpu < resourceLimits.Cpu && parentLimits.Cpu > 0)) {
                resourceLimits.Cpu = parentLimits.Cpu;
            }

            if (resourceLimits.Memory == 0 || (parentLimits.Memory < resourceLimits.Memory && parentLimits.Memory > 0)) {
                resourceLimits.Memory = parentLimits.Memory;
            }
        }

        return resourceLimits;
    }

    virtual TString GetAbsoluteName() const override
    {
        auto properties = WaitFor(Executor_->GetContainerProperties(
            Name_,
            std::vector<TString>{"absolute_name"}))
            .ValueOrThrow();

        return properties.at("absolute_name")
             .ValueOrThrow();
    }

    virtual TString GetStderr() const override
    {
        auto properties = WaitFor(Executor_->GetContainerProperties(
            Name_,
            std::vector<TString>{"stderr"}))
            .ValueOrThrow();

        return properties.at("stderr")
            .ValueOrThrow();
    }

    virtual void SetCpuShare(double cores) override
    {
        SetProperty("cpu_guarantee", ToString(cores) + "c");
    }

    virtual void SetCpuLimit(double cores) override
    {
        SetProperty("cpu_limit", ToString(cores) + "c");
    }

    virtual void SetEnablePorto(EEnablePorto enablePorto) override
    {
        EnablePorto_ = enablePorto;
    }

    virtual void EnableMemoryTracking() override
    {
        RequireMemoryController_ = true;
    }

    virtual void SetMemoryGuarantee(i64 memoryGuarantee) override
    {
        SetProperty("memory_guarantee", ToString(memoryGuarantee));
        RequireMemoryController_ = true;
    }

    virtual void SetIOWeight(double weight) override
    {
        SetProperty("io_weight", ToString(weight));
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        SetProperty("io_ops_limit", ToString(operations));
    }

    virtual void SetUser(const TString& user) override
    {
        User_ = user;
    }

    virtual TString GetName() const override
    {
        return Name_;
    }

    virtual pid_t GetPid() const override
    {
        auto pid = WaitFor(Executor_->GetContainerProperties(Name_, std::vector<TString>{"root_pid"}))
            .ValueOrThrow();
        return std::stoi(pid.at("root_pid")
            .ValueOrThrow());
    }

    virtual TFuture<int> Exec(
        const std::vector<const char*>& argv,
        const std::vector<const char*>& env) override
    {
        TStringBuilder commandBuilder;
        for (const auto* arg : argv) {
            commandBuilder.AppendString("'");
            commandBuilder.AppendString(EscapeForWordexp(arg));
            commandBuilder.AppendString("' ");
        }
        auto command = commandBuilder.Flush();

        YT_LOG_DEBUG("Executing Porto container (Command: %v)", command);
        SetProperty("command", command);

        if (User_) {
            SetProperty("user", *User_);
        } else {
            // NB(psushin): Make sure subcontainer starts with the same user.
            // For unknown reason in the cloud we've seen user_job containers with user=loadbase.
            SetProperty("user", ToString(::getuid()));
        }

        // Enable core dumps for all container instances.
        SetProperty("ulimit", "core: unlimited");

        std::vector<TString> controllers{
            "freezer",
            "cpu",
            "cpuacct",
            "net_cls",
            "blkio",
            "devices",
            "pids"
        };
        if (RequireMemoryController_) {
            controllers.push_back("memory");
        }
        SetProperty("controllers", JoinToString(controllers, AsStringBuf(";")));

        SetProperty("enable_porto", FormatEnablePorto(EnablePorto_));
        SetProperty("isolate", EnablePorto_ != EEnablePorto::Full ? "true" : "false");

        TStringBuilder envBuilder;
        for (const auto* arg : env) {
            envBuilder.AppendString(arg);
            envBuilder.AppendString(";");
        }
        SetProperty("env", envBuilder.Flush());

        // Wait for all pending actions - do not start real execution if
        // preparation has failed
        WaitForActions()
            .ThrowOnError();
        auto startAction = Executor_->StartContainer(Name_);

        // Wait for starting process - here we get error if exec has failed
        // i.e. no such file, execution bit, etc
        // In theory it is not necessarily to wait here, but in this case
        // error handling will be more difficult.
        auto error = WaitFor(startAction);
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION(EErrorCode::FailedToStartContainer, "Unable to start container")
                << error;
        }

        return Executor_->PollContainer(Name_);
    }

private:
    const TString Name_;
    const IPortoExecutorPtr Executor_;
    const bool AutoDestroy_;
    const NLogging::TLogger Logger;

    std::vector<TFuture<void>> Actions_;
    bool Destroyed_ = false;
    bool HasRoot_ = false;
    EEnablePorto EnablePorto_ = EEnablePorto::Full;
    bool RequireMemoryController_ = false;
    std::optional<TString> User_;

    mutable TSpinLock ContextSwitchMapLock_;
    mutable i64 TotalContextSwitches_ = 0;
    mutable THashMap<TString, i64> ContextSwitchMap_;

    TPortoInstance(
        const TString& name,
        IPortoExecutorPtr executor,
        bool autoDestroy)
        : Name_(name)
        , Executor_(executor)
        , AutoDestroy_(autoDestroy)
        , Logger(NLogging::TLogger(ContainersLogger)
            .AddTag("Container: %v", Name_))
    { }

    void SetProperty(const TString& key, const TString& value)
    {
        Actions_.push_back(Executor_->SetContainerProperty(Name_, key, value));
    }

    TError WaitForActions()
    {
        auto error = WaitFor(Combine(Actions_));
        Actions_.clear();
        return error;
    }

    static TString FormatEnablePorto(EEnablePorto value)
    {
        switch (value) {
            case EEnablePorto::None:    return "none";
            case EEnablePorto::Isolate: return "isolate";
            case EEnablePorto::Full:    return "full";
            default:                    YT_ABORT();
        }
    }

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

IInstancePtr CreatePortoInstance(const TString& name, IPortoExecutorPtr executor, bool autoDestroy)
{
    return TPortoInstance::Create(name, executor, autoDestroy);
}

IInstancePtr GetSelfPortoInstance(IPortoExecutorPtr executor)
{
    return TPortoInstance::GetSelf(executor);
}

IInstancePtr GetPortoInstance(IPortoExecutorPtr executor, const TString& name)
{
    return TPortoInstance::GetInstance(executor, name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

#endif
