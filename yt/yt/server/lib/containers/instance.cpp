#ifdef __linux__

#include "instance.h"

#include "porto_executor.h"
#include "private.h"

#include <yt/yt/ytlib/cgroup/cgroup.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <infra/porto/api/libporto.hpp>

#include <util/stream/file.h>

#include <util/string/cast.h>
#include <util/string/split.h>

#include <initializer_list>
#include <string>

namespace NYT::NContainers {

using namespace NCGroup;
using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

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
        BIND([] (const TString& in) { return ExtractSum(in, "hw", ":", ";");    } ) } },
    { EStatField::IOReadByte,       { "io_read",
        BIND([] (const TString& in) { return ExtractSum(in, "hw", ":", ";");    } ) } },
    { EStatField::IOWriteByte,      { "io_write",
        BIND([] (const TString& in) { return ExtractSum(in, "hw", ":", ";");    } ) } },
    { EStatField::MaxMemoryUsage,   { "memory.max_usage_in_bytes",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::MajorFaults,      { "major_faults",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
    { EStatField::ThreadCount,      { "thread_count",
        BIND([] (const TString& in) { return std::stol(in);                     } ) } },
};

std::optional<TString> GetParentName(const TString& name)
{
    if (name.empty()) {
        return std::nullopt;
    }

    auto slashPosition = name.rfind('/');
    if (slashPosition == TString::npos) {
        return "";
    }
    
    return name.substr(0, slashPosition);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TPortoInstanceLauncher
    : public IInstanceLauncher 
{
public:
    TPortoInstanceLauncher(const TString& name, IPortoExecutorPtr executor)
        : Executor_(std::move(executor))
        , Logger(ContainersLogger.WithTag("Container: %v", name))
    {
        Spec_.Name = name;
        Spec_.CGroupControllers = {
            "freezer",
            "cpu",
            "cpuacct",
            "net_cls",
            "blkio",
            "devices",
            "pids"
        };
    }

    virtual const TString& GetName() const override 
    {
        return Spec_.Name;
    }

    virtual bool HasRoot() const override 
    {
        return static_cast<bool>(Spec_.RootFS);
    }

    virtual void SetStdIn(const TString& inputPath) override
    {
        Spec_.StdinPath = inputPath;
    }

    virtual void SetStdOut(const TString& outPath) override
    {
        Spec_.StdoutPath = outPath;
    }

    virtual void SetStdErr(const TString& errorPath) override
    {
        Spec_.StderrPath = errorPath;
    }

    virtual void SetCwd(const TString& pwd) override
    {
        Spec_.CurrentWorkingDirectory = pwd;
    }

    virtual void SetCoreDumpHandler(const TString& handler) override
    {
        Spec_.CoreCommand = handler;
    }

    virtual void SetRoot(const TRootFS& rootFS) override
    {
        Spec_.RootFS = rootFS;
    }

    virtual void SetThreadLimit(i64 threadLimit) override
    {
        Spec_.ThreadLimit = threadLimit;
    }

    virtual void SetDevices(const std::vector<TDevice>& devices) override
    {
        Spec_.Devices = devices;
    }

    virtual void SetEnablePorto(EEnablePorto enablePorto) override
    {
        Spec_.EnablePorto = enablePorto;
    }

    virtual void SetIsolate(bool isolate) override
    {
        Spec_.Isolate = isolate;
    }

    virtual void EnableMemoryTracking() override
    {
        Spec_.CGroupControllers.push_back("memory");   
    }

    virtual void SetGroup(int groupId) override
    {
        Spec_.GroupId = groupId;
    }

    virtual void SetUser(const TString& user) override
    {
        Spec_.User = user;
    }

    virtual void SetIPAddresses(const std::vector<NNet::TIP6Address>& addresses) override
    {
        Spec_.IPAddresses = addresses;
    }

    virtual void SetHostName(const TString& hostName) override
    {
        Spec_.HostName = hostName;
    }

    virtual TFuture<IInstancePtr> Launch(
        const TString& path,
        const std::vector<TString>& args,
        const THashMap<TString, TString>& env) override
    {
        TStringBuilder commandBuilder;
        auto append = [&] (const auto& value) {
            commandBuilder.AppendString("'");
            commandBuilder.AppendString(NDetail::EscapeForWordexp(value.c_str()));
            commandBuilder.AppendString("' ");
        };	

        append(path);
        for (const auto& arg : args) {
            append(arg);
        }

        Spec_.Command = commandBuilder.Flush();
        YT_LOG_DEBUG("Executing Porto container (Name: %v, Command: %v)",
            Spec_.Name,
            Spec_.Command);

        Spec_.Env = env;

        auto onContainerCreated = [this, this_ = MakeStrong(this)] (const TError& error) -> IInstancePtr {
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION(EErrorCode::FailedToStartContainer, "Unable to start container")
                    << error;
            }

            return GetPortoInstance(Executor_, Spec_.Name);
        };

        return Executor_->CreateContainer(Spec_, /* start */ true)
            .Apply(BIND(onContainerCreated));
    }

private:
    IPortoExecutorPtr Executor_;
    TRunnableContainerSpec Spec_;
    const NLogging::TLogger Logger;
};

IInstanceLauncherPtr CreatePortoInstanceLauncher(const TString& name, IPortoExecutorPtr executor)
{
    return New<TPortoInstanceLauncher>(name, executor);
}

////////////////////////////////////////////////////////////////////////////////

class TPortoInstance
    : public IInstance
{
public:
    static IInstancePtr GetSelf(IPortoExecutorPtr executor)
    {
        return New<TPortoInstance>(GetSelfContainerName(executor), executor);
    }

    static IInstancePtr GetInstance(IPortoExecutorPtr executor, const TString& name)
    {
        return New<TPortoInstance>(name, executor);
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

    virtual TResourceUsage GetResourceUsage(const std::vector<EStatField>& fields) const override
    {
        std::vector<TString> properties;
        properties.push_back("absolute_name");

        bool contextSwitchesRequested = false;
        for (auto field : fields) {
            if (auto it = NDetail::PortoStatRules.find(field)) {
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
            auto ruleIt = NDetail::PortoStatRules.find(field);
            if (ruleIt == NDetail::PortoStatRules.end()) {
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
        auto subcontainers = WaitFor(Executor_->ListSubcontainers(Name_, /*includeRoot*/ true))
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
        static TString memoryLimitProperty = "memory_limit_total";
        static TString cpuLimitProperty = "cpu_limit_bound";
        properties.push_back(memoryLimitProperty);
        properties.push_back(cpuLimitProperty);

        auto responseOrError = WaitFor(Executor_->GetContainerProperties(Name_, properties));
        THROW_ERROR_EXCEPTION_IF_FAILED(responseOrError, "Failed to get Porto container resource limits");

        const auto& response = responseOrError.Value();
        const auto& memoryLimitRsp = response.at(memoryLimitProperty);

        THROW_ERROR_EXCEPTION_IF_FAILED(memoryLimitRsp, "Failed to get memory limit from Porto");

        i64 memoryLimit;

        if (!TryFromString<i64>(memoryLimitRsp.Value(), memoryLimit)) {
            THROW_ERROR_EXCEPTION("Failed to parse memory limit value from Porto")
                << TErrorAttribute(memoryLimitProperty, memoryLimitRsp.Value());
        }

        const auto& cpuLimitRsp = response.at(cpuLimitProperty);

        THROW_ERROR_EXCEPTION_IF_FAILED(cpuLimitRsp, "Failed to get CPU limit from Porto");

        double cpuLimit;

        YT_VERIFY(cpuLimitRsp.Value().EndsWith('c'));
        auto cpuLimitValue = TStringBuf(cpuLimitRsp.Value().begin(), cpuLimitRsp.Value().size() - 1);
        if (!TryFromString<double>(cpuLimitValue, cpuLimit)) {
            THROW_ERROR_EXCEPTION("Failed to parse CPU limit value from Porto")
                << TErrorAttribute(cpuLimitProperty, cpuLimitRsp.Value());
        }

        return TResourceLimits{cpuLimit, memoryLimit};
    }

    virtual void SetCpuGuarantee(double cores) override
    {
        SetProperty("cpu_guarantee", ToString(cores) + "c");
    }

    virtual void SetCpuLimit(double cores) override
    {
        SetProperty("cpu_limit", ToString(cores) + "c");
    }

    virtual void SetCpuWeight(double weight) override
    {
        SetProperty("cpu_weight", weight);
    }

    virtual void SetMemoryGuarantee(i64 memoryGuarantee) override
    {
        SetProperty("memory_guarantee", memoryGuarantee);
    }

    virtual void SetIOWeight(double weight) override
    {
        SetProperty("io_weight", weight);
    }

    virtual void SetIOThrottle(i64 operations) override
    {
        SetProperty("io_ops_limit", operations);
    }

    virtual TString GetStderr() const override	
    {	
        return *WaitFor(Executor_->GetContainerProperty(Name_, "stderr"))	
            .ValueOrThrow();	
    }

    virtual TString GetName() const override
    {
        return Name_;
    }

    virtual std::optional<TString> GetParentName() const override
    {
        return NDetail::GetParentName(Name_);
    }

    virtual pid_t GetPid() const override
    {
        auto pid = *WaitFor(Executor_->GetContainerProperty(Name_, "root_pid"))
            .ValueOrThrow();
        return std::stoi(pid);
    }

    virtual std::vector<pid_t> GetPids() const override
    {
        auto getPidCgroup = [&] (const TString& cgroups) {
            for (TStringBuf cgroup : StringSplitter(cgroups).SplitByString("; ")) {
                if (cgroup.StartsWith("pids:")) {
                    auto startPosition = cgroup.find('/');
                    YT_VERIFY(startPosition != TString::npos);
                    return cgroup.substr(startPosition);
                }
            }
            THROW_ERROR_EXCEPTION("Pids cgroup not found for container %Qv", GetName())
                << TErrorAttribute("cgroups", cgroups);
        };

        auto cgroups = *WaitFor(Executor_->GetContainerProperty(Name_, "cgroups"))
            .ValueOrThrow();
        // Porto returns full cgroup name, with mount prefix, such as "/sys/fs/cgroup/pids".
        auto instanceCgroup = getPidCgroup(cgroups);

        std::vector<pid_t> pids;
        for (auto pid : ListPids()) {
            std::map<TString, TString> cgroups;
            try {
                cgroups = GetProcessCGroups(pid);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to get CGroups for process (Pid: %v)", pid);
                continue;
            }

            // Pid cgroups are returned in short form. 
            auto processPidCgroup = cgroups["pids"];
            if (!processPidCgroup.empty() && instanceCgroup.EndsWith(processPidCgroup)) {
                pids.push_back(pid);
            }
        }

        return pids;
    }

    virtual TFuture<void> Wait() override 
    {
        return Executor_->PollContainer(Name_)
            .Apply(BIND([] (int status) {
                StatusToError(status)
                    .ThrowOnError();
            }));
    }

private:
    const TString Name_;
    const IPortoExecutorPtr Executor_;
    const NLogging::TLogger Logger;

    bool Destroyed_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, ContextSwitchMapLock_);
    mutable i64 TotalContextSwitches_ = 0;
    mutable THashMap<TString, i64> ContextSwitchMap_;

    TPortoInstance(
        const TString name,
        IPortoExecutorPtr executor)
        : Name_(std::move(name))
        , Executor_(std::move(executor))
        , Logger(ContainersLogger.WithTag("Container: %v", Name_))
    { }

    void SetProperty(const TString& key, const TString& value)
    {
        WaitFor(Executor_->SetContainerProperty(Name_, key, value))
            .ThrowOnError();
    }

    void SetProperty(const TString& key, i64 value)
    {
        SetProperty(key, ToString(value));
    }

    void SetProperty(const TString& key, double value)
    {
        SetProperty(key, ToString(value));
    }

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

TString GetSelfContainerName(const IPortoExecutorPtr& executor)
{
    try {
        auto properties = WaitFor(executor->GetContainerProperties(
            "self",
            std::vector<TString>{"absolute_name", "absolute_namespace"}))
            .ValueOrThrow();

        auto absoluteName = properties.at("absolute_name")
            .ValueOrThrow();
        auto absoluteNamespace = properties.at("absolute_namespace")
            .ValueOrThrow();

        if (absoluteName == "/") {
            return absoluteName;
        }

        if (absoluteName.length() < absoluteNamespace.length()) {
            YT_VERIFY(absoluteName + "/" == absoluteNamespace);
            return "";    
        } else {
            YT_VERIFY(absoluteName.StartsWith(absoluteNamespace));
            return absoluteName.substr(absoluteNamespace.length());
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to get name for container \"self\"")
            << ex;
    }
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
