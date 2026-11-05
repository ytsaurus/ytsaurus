#include "porto_executor.h"
#include "config.h"

#include "porto_helpers.h"
#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/porto/proto/rpc.pb.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <string>

namespace NYT::NContainers {

using namespace NConcurrency;
using Porto::EError;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

constinit const auto Logger = ContainersLogger;
static constexpr auto RetryInterval = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

static const std::vector<TDevice> DefaultContainerDevices = {
    {
        .DeviceName = "/dev/kvm",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/fuse",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/null",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/zero",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/full",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/random",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/urandom",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/tty",
        .Access = "rw",
    },
    {
        .DeviceName = "/dev/null",
        .Access = "rw",
        // It is necessary for the consistency of default devices in the container, but access must be denied.
        // See porto/src/device.cpp::TDevices::InitDefault and https://docs.kernel.org/admin-guide/serial-console.html.
        .Path = "/dev/console",
    }
};

////////////////////////////////////////////////////////////////////////////////

std::string PortoErrorCodeFormatter(int code)
{
    return TEnumTraits<EPortoErrorCode>::ToString(static_cast<EPortoErrorCode>(code));
}

YT_DEFINE_ERROR_CODE_RANGE(12000, 13999, "NYT::NContainers::EPortoErrorCode", PortoErrorCodeFormatter);

////////////////////////////////////////////////////////////////////////////////

EPortoErrorCode ConvertPortoErrorCode(EError portoError)
{
    return static_cast<EPortoErrorCode>(PortoErrorCodeBase + portoError);
}

bool IsRetriableErrorCode(EPortoErrorCode error, bool idempotent)
{
    return
        error == EPortoErrorCode::Unknown ||
        // TODO(babenko): it's not obvious that we can always retry SocketError
        // but this is how it has used to work for a while.
        error == EPortoErrorCode::SocketError ||
        error == EPortoErrorCode::SocketTimeout && idempotent;
}

THashMap<std::string, TErrorOr<std::string>> ParsePortoGetResponse(
    const Porto::TGetResponse_TContainerGetListResponse& response)
{
    THashMap<std::string, TErrorOr<std::string>> result;
    for (const auto& property : response.keyval()) {
        if (property.error() == EError::Success) {
            result[property.variable()] = std::string(property.value());
        } else {
            result[property.variable()] = TError(ConvertPortoErrorCode(property.error()), TRuntimeFormat(property.errormsg()))
                .With("porto_error", ConvertPortoErrorCode(property.error()));
        }
    }
    return result;
}

THashMap<std::string, TErrorOr<std::string>> ParseSinglePortoGetResponse(
    const std::string& name,
    const Porto::TGetResponse& getResponse)
{
    for (const auto& container : getResponse.list()) {
        if (container.name() == name) {
            return ParsePortoGetResponse(container);
        }
    }
    THROW_ERROR_EXCEPTION("Unable to get properties from Porto")
        .With("container", name);
}

THashMap<std::string, THashMap<std::string, TErrorOr<std::string>>> ParseMultiplePortoGetResponse(
    const Porto::TGetResponse& getResponse)
{
    THashMap<std::string, THashMap<std::string, TErrorOr<std::string>>> result;
    for (const auto& container : getResponse.list()) {
        result[container.name()] = ParsePortoGetResponse(container);
    }
    return result;
}

std::string FormatEnablePorto(EEnablePorto value)
{
    switch (value) {
        case EEnablePorto::None:    return "none";
        case EEnablePorto::Isolate: return "isolate";
        case EEnablePorto::Full:    return "full";
        default:                    YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<std::unique_ptr<Porto::TPortoApi>> CreateApis(int apiCount)
{
    std::vector<std::unique_ptr<Porto::TPortoApi>> apis;
    apis.reserve(apiCount);
    for (int index = 0; index < apiCount; ++index) {
        apis.push_back(std::make_unique<Porto::TPortoApi>());
    }
    return apis;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutor
    : public IPortoExecutor
{
public:
    TPortoExecutor(
        TPortoExecutorDynamicConfigPtr config,
        std::string threadNameSuffix,
        const NProfiling::TProfiler& profiler,
        int apiThreadCount)
        : Config_(std::move(config))
        // A single-thread queue serializing Porto poll, which talks to the
        // Porto API directly on this thread; all other API calls go to the
        // thread pool below.
        , PollQueue_(New<TActionQueue>(Format("Porto:%v", threadNameSuffix)))
        , Profiler_(profiler)
        , Apis_(CreateApis(apiThreadCount + 1))
        , ThreadPool_(CreateThreadPool(
            apiThreadCount,
            Format("Porto:%v:Api", threadNameSuffix)))
        , ApiInvoker_(ThreadPool_->GetInvoker())
        , PollExecutor_(New<TPeriodicExecutor>(
            PollQueue_->GetInvoker(),
            BIND(&TPortoExecutor::DoPoll, MakeWeak(this)),
            Config_->PollPeriod))
    {
        DynamicConfig_.Store(New<TPortoExecutorDynamicConfig>());
        EffectiveConfig_.Store(Config_);

        FreeApis_.reserve(Apis_.size());
        for (const auto& api : Apis_) {
            FreeApis_.push_back(api.get());
        }

        Profiler_.AddFuncGauge("/volume_surplus", MakeStrong(this), [this] {
            return VolumeSurplus_.load();
        });

        Profiler_.AddFuncGauge("/api_handles_total", MakeStrong(this), [this] {
            return ssize(Apis_);
        });

        Profiler_.AddFuncGauge("/api_handles_free", MakeStrong(this), [this] {
            auto guard = Guard(ApiPoolLock_);
            return ssize(FreeApis_);
        });

        PollExecutor_->Start();
    }

    void SubscribeFailed(const TCallback<void(const TError&)>& callback) override
    {
        Failed_.Subscribe(callback);
    }

    void UnsubscribeFailed(const TCallback<void(const TError&)>& callback) override
    {
        Failed_.Unsubscribe(callback);
    }

    void OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig) override
    {
        YT_TLOG_DEBUG("Reconfiguring Porto")
            .With("ApiTimeout", newConfig->ApiTimeout.Seconds())
            .With("ApiDiskTimeout", newConfig->ApiDiskTimeout.Seconds());

        DynamicConfig_.Store(newConfig);
        EffectiveConfig_.Store(newConfig);
    }

private:
    template <class TMethod, class... TArgs>
    auto ExecutePortoApiAction(
        TMethod method,
        const std::string& command,
        TArgs&&... args)
    {
        YT_TLOG_DEBUG("Enqueue Porto API action")
            .With("Command", command);

        return BIND(method, MakeStrong(this), std::forward<TArgs>(args)...)
            .AsyncVia(ApiInvoker_)
            .Run();
    };

public:
    TFuture<void> CreateContainer(const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoCreateContainer,
            "CreateContainer",
            container);
    }

    TFuture<void> CreateContainer(const TRunnableContainerSpec& containerSpec, bool start) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoCreateContainerFromSpec,
            "CreateContainerFromSpec",
            containerSpec,
            start);
    }

    TFuture<std::optional<std::string>> GetContainerProperty(
        const std::string& container,
        const std::string& property) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoGetContainerProperty,
            "GetContainerProperty",
            container,
            property);
    }

    TFuture<THashMap<std::string, TErrorOr<std::string>>> GetContainerProperties(
        const std::string& container,
        const std::vector<std::string>& properties) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoGetContainerProperties,
            "GetContainerProperty",
            container,
            properties);
    }

    TFuture<THashMap<std::string, THashMap<std::string, TErrorOr<std::string>>>> GetContainerProperties(
        const std::vector<std::string>& containers,
        const std::vector<std::string>& properties) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoGetContainerMultipleProperties,
            "GetContainerProperty",
            containers,
            properties);
    }

    TFuture<THashMap<std::string, i64>> GetContainerMetrics(
        const std::vector<std::string>& containers,
        const std::string& metric) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoGetContainerMetrics,
            "GetContainerMetrics",
            containers,
            metric);
    }

    TFuture<void> SetContainerProperty(
        const std::string& container,
        const std::string& property,
        const std::string& value) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoSetContainerProperty,
            "SetContainerProperty",
            container,
            property,
            value);
    }

    TFuture<void> DestroyContainer(const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoDestroyContainer,
            "DestroyContainer",
            container);
    }

    TFuture<void> StopContainer(const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoStopContainer,
            "StopContainer",
            container);
    }

    TFuture<void> StartContainer(const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoStartContainer,
            "StartContainer",
            container);
    }

    TFuture<void> RespawnContainer(const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoRespawnContainer,
            "RespawnContainer",
            container);
    }

    TFuture<std::string> ConvertPath(const std::string& path, const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoConvertPath,
            "ConvertPath",
            path,
            container);
    }

    TFuture<void> KillContainer(const std::string& container, int signal) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoKillContainer,
            "KillContainer",
            container,
            signal);
    }

    TFuture<std::vector<std::string>> ListSubcontainers(
        const std::string& rootContainer,
        bool includeRoot) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoListSubcontainers,
            "ListSubcontainers",
            rootContainer,
            includeRoot);
    }

    TFuture<int> PollContainer(const std::string& container) override
    {
        YT_TLOG_DEBUG("Enqueue Porto poll action")
            .With("Command", "PollContainer");

        // NB: does not talk to Porto API, only mutates polling bookkeeping,
        // hence runs on #PollQueue_ to stay serialized with DoPoll.
        return BIND(&TPortoExecutor::DoPollContainer, MakeStrong(this), container)
            .AsyncVia(PollQueue_->GetInvoker())
            .Run();
    }

    // This method allocates Porto "resources", so it should be uncancellable.
    TFuture<std::string> CreateVolume(
        const std::string& path,
        const THashMap<std::string, std::string>& properties) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoCreateVolume,
            "CreateVolume",
            path,
            properties)
            .ToUncancelable();
    }

    // This method allocates Porto "resources", so it should be uncancellable.
    TFuture<void> LinkVolume(
        const std::string& path,
        const std::string& name,
        const std::string& target) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoLinkVolume,
            "LinkVolume",
            path,
            name,
            target)
            .ToUncancelable();
    }

    // This method deallocates Porto "resources", so it should be uncancellable.
    TFuture<void> UnlinkVolume(
        const std::string& path,
        const std::string& name,
        const std::string& target) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoUnlinkVolume,
            "UnlinkVolume",
            path,
            name,
            target)
            .ToUncancelable();
    }

    TFuture<std::vector<std::string>> ListVolumePaths() override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoListVolumePaths,
            "ListVolumePaths");
    }

    TFuture<std::vector<TVolumeSpec>> GetVolumes() override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoGetVolumes,
            "GetVolume");
    }

    // This method allocates Porto "resources", so it should be uncancellable.
    TFuture<void> ImportLayer(
        const std::string& archivePath,
        const std::string& layerId,
        const std::string& place,
        const std::string& container) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoImportLayer,
            "ImportLayer",
            archivePath,
            layerId,
            place,
            container)
            .ToUncancelable();
    }

    // This method deallocates Porto "resources", so it should be uncancellable.
    TFuture<void> RemoveLayer(const std::string& layerId, const std::string& place, bool async) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoRemoveLayer,
            "RemoveLayer",
            layerId,
            place,
            async)
            .ToUncancelable();
    }

    TFuture<std::vector<std::string>> ListLayers(const std::string& place) override
    {
        return ExecutePortoApiAction(
            &TPortoExecutor::DoListLayers,
            "ListLayers",
            place);
    }

    i64 GetLayerSurplus() const override
    {
        return LayerSurplus_.load(std::memory_order::relaxed);
    }

private:
    const TPortoExecutorDynamicConfigPtr Config_;
    const TActionQueuePtr PollQueue_;
    const NProfiling::TProfiler Profiler_;

    //! A pool of Porto API handles; each in-flight API attempt borrows one in
    //! #ExecuteApiCall. #ApiInvoker_ runs the attempts on #ThreadPool_ with
    //! |apiThreadCount| threads, and one extra handle serves the poll thread
    //! hosted by #PollQueue_.
    const std::vector<std::unique_ptr<Porto::TPortoApi>> Apis_;
    const IThreadPoolPtr ThreadPool_;
    const IInvokerPtr ApiInvoker_;

    const TPeriodicExecutorPtr PollExecutor_;
    TAtomicIntrusivePtr<TPortoExecutorDynamicConfig> DynamicConfig_;
    //! Seeded from #Config_ (unlike #DynamicConfig_, which starts with defaults);
    //! #AcquireApi applies its timeouts to each borrowed handle.
    TAtomicIntrusivePtr<TPortoExecutorDynamicConfig> EffectiveConfig_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, ApiPoolLock_);
    //! Free handles from #Apis_, guarded by #ApiPoolLock_.
    std::vector<Porto::TPortoApi*> FreeApis_;

    std::vector<std::string> Containers_;
    THashMap<std::string, TPromise<int>> ContainerMap_;
    TSingleShotCallbackList<void(const TError&)> Failed_;

    //! Gauge counting actual difference between the number of created and unlinked volumes.
    std::atomic<i64> VolumeSurplus_ = 0;
    //! Gauge counting actual difference between the number of imported and removed layers.
    std::atomic<i64> LayerSurplus_ = 0;

    struct TCommandEntry
    {
        explicit TCommandEntry(const NProfiling::TProfiler& registry)
            : TimeGauge(registry.Timer("/command_time"))
            , RetryCounter(registry.Counter("/command_retries"))
            , SuccessCounter(registry.Counter("/command_successes"))
            , FailureCounter(registry.Counter("/command_failures"))
        { }

        NProfiling::TEventTimer TimeGauge;
        NProfiling::TCounter RetryCounter;
        NProfiling::TCounter SuccessCounter;
        NProfiling::TCounter FailureCounter;
    };

    YT_DECLARE_SPIN_LOCK(TSpinLock, CommandLock_);
    THashMap<std::string, TCommandEntry> CommandToEntry_;

    static const std::vector<std::string> ContainerRequestVars_;

    bool IsTestPortoFailureEnabled() const
    {
        auto config = DynamicConfig_.Acquire();
        return config->EnableTestPortoFailures;
    }

    bool IsTestPortoTimeout() const
    {
        auto config = DynamicConfig_.Acquire();
        return config->EnableTestPortoNotResponding;
    }

    EPortoErrorCode GetFailedStubError() const
    {
        auto config = DynamicConfig_.Acquire();
        return config->StubErrorCode;
    }

    static TError CreatePortoError(EPortoErrorCode errorCode, const std::string& message)
    {
        return TError(errorCode, "Porto API error")
            .With("original_porto_error_code", static_cast<int>(errorCode) - PortoErrorCodeBase)
            .With("porto_error_message", message);
    }

    //! Must only be called on an #ApiInvoker_ thread or the poll thread;
    //! the number of concurrent callers (|apiThreadCount| API threads plus
    //! the poll thread) never exceeds the pool size, so a handle is always available.
    Porto::TPortoApi* AcquireApi()
    {
        Porto::TPortoApi* api;
        {
            auto guard = Guard(ApiPoolLock_);
            YT_VERIFY(!FreeApis_.empty());
            api = FreeApis_.back();
            FreeApis_.pop_back();
        }

        // NB: TPortoApi is not thread-safe, so timeouts are (re)applied here,
        // while the handle is exclusively owned, rather than in OnDynamicConfigChanged.
        auto config = EffectiveConfig_.Acquire();
        auto apiTimeout = static_cast<int>(config->ApiTimeout.Seconds());
        auto diskTimeout = static_cast<int>(config->ApiDiskTimeout.Seconds());
        if (api->GetTimeout() != apiTimeout) {
            api->SetTimeout(apiTimeout);
        }
        if (api->GetDiskTimeout() != diskTimeout) {
            api->SetDiskTimeout(diskTimeout);
        }

        return api;
    }

    void ReleaseApi(Porto::TPortoApi* api)
    {
        auto guard = Guard(ApiPoolLock_);
        FreeApis_.push_back(api);
    }

    THashMap<std::string, TErrorOr<std::string>> DoGetContainerProperties(
        const std::string& container,
        const std::vector<std::string>& properties)
    {
        auto response = DoRequestContainerProperties({container}, properties);
        return ParseSinglePortoGetResponse(container, response);
    }

    THashMap<std::string, THashMap<std::string, TErrorOr<std::string>>> DoGetContainerMultipleProperties(
        const std::vector<std::string>& containers,
        const std::vector<std::string>& properties)
    {
        auto response = DoRequestContainerProperties(containers, properties);
        return ParseMultiplePortoGetResponse(response);
    }

    std::optional<std::string> DoGetContainerProperty(
        const std::string& container,
        const std::string& property)
    {
        auto response = DoRequestContainerProperties({container}, {property});
        auto parsedResponse = ParseSinglePortoGetResponse(container, response);
        auto it = parsedResponse.find(property);
        if (it == parsedResponse.end()) {
            return std::nullopt;
        } else {
            return it->second.ValueOrThrow();
        }
    }

    void DoCreateContainer(const std::string& container)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->Create(TString(container)); },
            "Create",
            /*idempotent*/ false);
    }

    void DoCreateContainerFromSpec(const TRunnableContainerSpec& spec, bool start)
    {
        Porto::TContainerSpec portoSpec;

        // Required properties.
        portoSpec.set_name(spec.Name);
        portoSpec.set_command(spec.Command);

        portoSpec.set_enable_porto(FormatEnablePorto(spec.EnablePorto));
        portoSpec.set_isolate(spec.Isolate);
        portoSpec.set_enable_fuse(spec.EnableFuse);

        if (spec.StdinPath) {
            portoSpec.set_stdin_path(*spec.StdinPath);
        }
        if (spec.StdoutPath) {
            portoSpec.set_stdout_path(*spec.StdoutPath);
        }
        if (spec.StderrPath) {
            portoSpec.set_stderr_path(*spec.StderrPath);
        }

        if (spec.CurrentWorkingDirectory) {
            portoSpec.set_cwd(*spec.CurrentWorkingDirectory);
        }

        if (spec.CoreCommand) {
            portoSpec.set_core_command(*spec.CoreCommand);
        }
        if (spec.User) {
            portoSpec.set_user(*spec.User);
        }

        // Useful for jobs, where we operate with numeric group ids.
        if (spec.GroupId) {
            portoSpec.set_group(ToString(*spec.GroupId));
        }

        if (spec.CpuWeight) {
            portoSpec.set_cpu_weight(*spec.CpuWeight);
        }

        if (spec.ThreadLimit) {
            portoSpec.set_thread_limit(*spec.ThreadLimit);
        }

        if (spec.HostName) {
            // To get a reasonable and unique host name inside container.
            portoSpec.set_hostname(*spec.HostName);
            if (!spec.IPAddresses.empty()) {
                const auto& address = spec.IPAddresses[0];
                auto etcHosts = Format("%v %v\n", address, *spec.HostName);
                // To be able to resolve hostname into IP inside container.
                portoSpec.set_etc_hosts(etcHosts);
            }
        }

        if (spec.DisableNetwork) {
            auto* netConfig = portoSpec.mutable_net()->add_cfg();
            netConfig->set_opt("none");
        } else if (!spec.IPAddresses.empty() && Config_->EnableNetworkIsolation) {
            // This label is intended for HBF-agent: YT-12512.
            auto* label = portoSpec.mutable_labels()->add_map();
            label->set_key("HBF.ignore_address");
            label->set_val("1");

            auto* netConfig = portoSpec.mutable_net()->add_cfg();
            netConfig->set_opt("L3");
            netConfig->add_arg(spec.NetworkInterface.value_or(std::string(DefaultPortoNetworkInterface)));

            for (const auto& address : spec.IPAddresses) {
                auto* ipConfig = portoSpec.mutable_ip()->add_cfg();
                ipConfig->set_dev(spec.NetworkInterface.value_or(std::string(DefaultPortoNetworkInterface)));
                ipConfig->set_ip(ToString(address));
            }

            if (spec.EnableNat64) {
                // Behave like nanny does.
                portoSpec.set_resolv_conf("nameserver fd64::1;nameserver 2a02:6b8:0:3400::5005;options attempts:1 timeout:1");
            }
        }

        for (const auto& [key, value] : spec.Labels) {
            auto* map = portoSpec.mutable_labels()->add_map();
            map->set_key(key);
            map->set_val(value);
        }

        for (const auto& [name, value] : spec.Env) {
            auto* var = portoSpec.mutable_env()->add_var();
            var->set_name(name);
            var->set_value(value);
        }

        for (const auto& controller : spec.CGroupControllers) {
            portoSpec.mutable_controllers()->add_controller(controller);
        }

        const auto& devices = !spec.Devices.empty()
            ? spec.Devices
            : DefaultContainerDevices;

        for (const auto& device : devices) {
            if (NFS::Exists(device.DeviceName)) {
                auto* portoDevice = portoSpec.mutable_devices()->add_device();
                portoDevice->set_device(device.DeviceName);
                portoDevice->set_access(device.Access);

                if (device.Path) {
                    portoDevice->set_path(device.Path.value());
                }
            }
        }

        auto addBind = [&] (const TBind& bind) {
            auto* portoBind = portoSpec.mutable_bind()->add_bind();
            portoBind->set_target(bind.TargetPath);
            portoBind->set_source(bind.SourcePath);
            portoBind->add_flag(bind.ReadOnly ? "ro" : "rw");
        };

        if (spec.RootFS) {
            portoSpec.set_root_readonly(spec.RootFS->IsRootReadOnly);
            portoSpec.set_root(spec.RootFS->RootPath);

            // COMPAT(krasovav)
            for (const auto& bind : spec.RootFS->Binds) {
                addBind(bind);
            }
        }

        for (const auto& bind : spec.Binds) {
            addBind(bind);
        }

        for (const auto& place : spec.Places) {
            portoSpec.mutable_place()->add_cfg()->set_place(place);
        }

        {
            auto* ulimit = portoSpec.mutable_ulimit()->add_ulimit();
            ulimit->set_type("core");
            if (spec.EnableCoreDumps) {
                ulimit->set_unlimited(true);
            } else {
                ulimit->set_hard(0);
                ulimit->set_soft(0);
            }
        }

        // Set some universal defaults.
        portoSpec.set_oom_is_fatal(false);

        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->CreateFromSpec(portoSpec, {}, start); },
            "CreateFromSpec",
            /*idempotent*/ false);
    }

    void DoSetContainerProperty(const std::string& container, const std::string& property, const std::string& value)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->SetProperty(TString(container), TString(property), TString(value)); },
            "SetProperty",
            /*idempotent*/ true);
    }

    void DoDestroyContainer(const std::string& container)
    {
        try {
            // TODO(babenko): drop cast once Porto API accepts std::string.
            ExecuteApiCall(
                [&] (Porto::TPortoApi* api) { return api->Destroy(TString(container)); },
                "Destroy",
                /*idempotent*/ true);
        } catch (const TErrorException& ex) {
            if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                throw;
            }
        }
    }

    void DoStopContainer(const std::string& container)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->Stop(TString(container)); },
            "Stop",
            /*idempotent*/ true);
    }

    void DoStartContainer(const std::string& container)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->Start(TString(container)); },
            "Start",
            /*idempotent*/ false);
    }

    void DoRespawnContainer(const std::string& container)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->Respawn(TString(container)); },
            "Respawn",
            /*idempotent*/ false);
    }

    std::string DoConvertPath(const std::string& path, const std::string& container)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        TString result;
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->ConvertPath(TString(path), TString(container), "self", result); },
            "ConvertPath",
            /*idempotent*/ true);
        return result;
    }

    void DoKillContainer(const std::string& container, int signal)
    {
        // TODO(babenko): drop cast once Porto API accepts std::string.
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->Kill(TString(container), signal); },
            "Kill",
            /*idempotent*/ false);
    }

    std::vector<std::string> DoListSubcontainers(const std::string& rootContainer, bool includeRoot)
    {
        Porto::TListContainersRequest req;
        auto filter = req.add_filters();
        filter->set_name(rootContainer + "/*");
        if (includeRoot) {
            auto rootFilter = req.add_filters();
            rootFilter->set_name(rootContainer);
        }
        auto fieldOptions = req.mutable_field_options();
        fieldOptions->add_properties("absolute_name");
        TVector<Porto::TContainer> containers;
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->ListContainersBy(req, containers); },
            "ListContainersBy",
            /*idempotent*/ true);

        std::vector<std::string> containerNames;
        containerNames.reserve(containers.size());
        for (const auto& container : containers) {
            const auto& absoluteName = container.status().absolute_name();
            if (!absoluteName.empty()) {
                containerNames.push_back(absoluteName);
            }
        }
        return containerNames;
    }

    TFuture<int> DoPollContainer(const std::string& container)
    {
        auto [it, inserted] = ContainerMap_.emplace(container, NewPromise<int>());
        if (!inserted) {
            YT_TLOG_WARNING("Container already added for polling")
                .With("Container", container);
        } else {
            Containers_.push_back(container);
        }
        return it->second.ToFuture();
    }

    Porto::TGetResponse DoRequestContainerProperties(
        const std::vector<std::string>& containers,
        const std::vector<std::string>& vars)
    {
        TVector<TString> containers_(containers.begin(), containers.end());
        TVector<TString> vars_(vars.begin(), vars.end());

        // NB: the response points into the handle's own buffer, so it must be
        // copied out while the handle is still held by #ExecuteApiCall.
        Porto::TGetResponse response;

        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) {
                const auto* getResponse = api->Get(containers_, vars_);
                if (!getResponse) {
                    return EError::Unknown;
                }
                response = *getResponse;
                return EError::Success;
            },
            "Get",
            /*idempotent*/ true);

        return response;
    }

    THashMap<std::string, i64> DoGetContainerMetrics(
        const std::vector<std::string>& containers,
        const std::string& metric)
    {
        TVector<TString> containers_(containers.begin(), containers.end());

        TMap<TString, uint64_t> result;

        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->GetProcMetric(containers_, TString(metric), result); },
            "GetProcMetric",
            /*idempotent*/ true);

        return {result.begin(), result.end()};
    }

    void DoPoll()
    {
        try {
            if (Containers_.empty()) {
                return;
            }

            auto getResponse = DoRequestContainerProperties(Containers_, ContainerRequestVars_);

            if (getResponse.list().empty()) {
                return;
            }

            auto getProperty = [] (
                const Porto::TGetResponse::TContainerGetListResponse& container,
                const std::string& name) -> Porto::TGetResponse::TContainerGetValueResponse
            {
                for (const auto& property : container.keyval()) {
                    if (property.variable() == name) {
                        return property;
                    }
                }

                return {};
            };

            for (const auto& container : getResponse.list()) {
                auto state = getProperty(container, "state");
                if (state.error() == EError::ContainerDoesNotExist) {
                    HandleResult(container.name(), state);
                } else if (state.value() == "dead" || state.value() == "stopped") {
                    HandleResult(container.name(), getProperty(container, "exit_status"));
                }
                // TODO(dcherednik): other states
            }
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Fatal exception occurred while polling Porto")
                .With(ex);
            Failed_.Fire(TError(ex));
        }
    }

    std::string DoCreateVolume(
        const std::string& path,
        const THashMap<std::string, std::string>& properties)
    {
        auto volume = TString(path);
        TMap<TString, TString> propertyMap(properties.begin(), properties.end());
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->CreateVolume(volume, propertyMap); },
            "CreateVolume",
            /*idempotent*/ false);

        // TODO(yuryalekseev): Remove this check when rbind volume is removed.
        auto it = properties.find("backend");
        if (it == properties.end() || it->second != "rbind") {
            VolumeSurplus_ += 1;
        }
        return volume;
    }

    void DoLinkVolume(const std::string& path, const std::string& container, const std::string& target)
    {
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->LinkVolume(TString(path), TString(container), TString(target)); },
            "LinkVolume",
            /*idempotent*/ false);
    }

    void DoUnlinkVolume(const std::string& path, const std::string& container, const std::string& target)
    {
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->UnlinkVolume(TString(path), TString(container), TString(target)); },
            "UnlinkVolume",
            /*idempotent*/ false);

        if (target == AnyTarget) {
            VolumeSurplus_ -= 1;
        }
    }

    std::vector<std::string> DoListVolumePaths()
    {
        TVector<TString> volumes;
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->ListVolumes(volumes); },
            "ListVolume",
            /*idempotent*/ true);
        return {volumes.begin(), volumes.end()};
    }

    std::vector<TVolumeSpec> DoGetVolumes()
    {
        std::vector<TVolumeSpec> specs;

        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) {
                // NB: the response points into the handle's own buffer, so parse it
                // while the handle is still held by this callback.
                const auto* getResponse = api->GetVolumes();
                if (!getResponse) {
                    return EError::Unknown;
                }

                specs.reserve(getResponse->volume_size());
                for (const auto& spec : getResponse->volume()) {
                    specs.push_back(TVolumeSpec{
                        .Id = spec.id(),
                        .Path = spec.path(),
                        .Backend = spec.backend(),
                        .State = spec.state(),
                    });
                }
                return EError::Success;
            },
            "GetVolume",
            /*idempotent*/ true);

        return specs;
    }

    void DoImportLayer(
        const std::string& archivePath,
        const std::string& layerId,
        const std::string& place,
        const std::string& container)
    {
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->ImportLayer(TString(layerId), TString(archivePath), false, TString(place), "", TString(container)); },
            "ImportLayer",
            /*idempotent*/ false);
        LayerSurplus_ += 1;
    }

    void DoRemoveLayer(const std::string& layerId, const std::string& place, bool async)
    {
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->RemoveLayer(TString(layerId), TString(place), async); },
            "RemoveLayer",
            /*idempotent*/ false);
        LayerSurplus_ -= 1;
    }

    std::vector<std::string> DoListLayers(const std::string& place)
    {
        TVector<TString> layers;
        ExecuteApiCall(
            [&] (Porto::TPortoApi* api) { return api->ListLayers(layers, TString(place)); },
            "ListLayers",
            /*idempotent*/ true);
        return {layers.begin(), layers.end()};
    }

    TCommandEntry* GetCommandEntry(const std::string& command)
    {
        auto guard = Guard(CommandLock_);
        if (auto it = CommandToEntry_.find(command)) {
            return &it->second;
        }
        return &CommandToEntry_.emplace(command, TCommandEntry(Profiler_.WithTag("command", command))).first->second;
    }

    void ExecuteApiCall(
        std::function<EError(Porto::TPortoApi*)> callback,
        const std::string& command,
        bool idempotent)
    {
        YT_TLOG_DEBUG("Porto API call started")
            .With("Command", command);

        if (IsTestPortoTimeout()) {
            YT_TLOG_DEBUG("Testing Porto timeout")
                .With("Command", command);

            auto config = DynamicConfig_.Acquire();
            TDelayedExecutor::WaitForDuration(config->ApiTimeout);

            THROW_ERROR CreatePortoError(GetFailedStubError(), "Porto timeout");
        }

        if (IsTestPortoFailureEnabled()) {
            YT_TLOG_DEBUG("Testing Porto failure")
                .With("Command", command);
            THROW_ERROR CreatePortoError(GetFailedStubError(), "Porto stub error");
        }

        auto* entry = GetCommandEntry(command);
        auto startTime = NProfiling::GetInstant();
        while (true) {
            EError error;

            {
                // The handle is held for a single attempt only: this fiber may
                // yield for the retry delay below, and holding the handle
                // across that would let other actions drain the pool.
                auto* api = AcquireApi();
                auto apiGuard = Finally([&] {
                    ReleaseApi(api);
                });

                {
                    NProfiling::TWallTimer timer;
                    error = callback(api);
                    entry->TimeGauge.Record(timer.GetElapsedTime());
                }

                if (error == EError::Success) {
                    entry->SuccessCounter.Increment();
                    break;
                }

                entry->FailureCounter.Increment();
                HandleApiError(command, startTime, idempotent, api);
            }

            YT_TLOG_DEBUG("Sleeping and retrying Porto API call")
                .With("Command", command);
            entry->RetryCounter.Increment();

            TDelayedExecutor::WaitForDuration(RetryInterval);
        }

        YT_TLOG_DEBUG("Porto API call completed")
            .With("Command", command);
    }

    void HandleApiError(
        const std::string& command,
        TInstant startTime,
        bool idempotent,
        Porto::TPortoApi* api)
    {
        TString errorMessage;
        auto error = ConvertPortoErrorCode(api->GetLastError(errorMessage));

        // These errors are typical during job cleanup: we might try to kill a container that is already stopped.
        bool debug = (error == EPortoErrorCode::ContainerDoesNotExist || error == EPortoErrorCode::InvalidState);
        YT_TLOG_EVENT(
            Logger,
            debug ? NLogging::ELogLevel::Debug : NLogging::ELogLevel::Error,
            "Porto API call error")
            .With("Error", error)
            .With("Command", command)
            .With("Message", errorMessage);

        if (!IsRetriableErrorCode(error, idempotent) || NProfiling::GetInstant() - startTime > Config_->RetriesTimeout) {
            THROW_ERROR CreatePortoError(error, errorMessage);
        }
    }

    void HandleResult(const std::string& container, const Porto::TGetResponse::TContainerGetValueResponse& rsp)
    {
        auto portoErrorCode = ConvertPortoErrorCode(rsp.error());
        auto it = ContainerMap_.find(container);
        if (it == ContainerMap_.end()) {
            YT_TLOG_ERROR("Got an unexpected container")
                .With("Container", container)
                .With("ResponseError", portoErrorCode)
                .With("ErrorMessage", rsp.errormsg())
                .With("Value", rsp.value());
            return;
        } else {
            if (portoErrorCode != EPortoErrorCode::Success) {
                YT_TLOG_ERROR("Container finished with Porto API error")
                    .With("Container", container)
                    .With("ResponseError", portoErrorCode)
                    .With("ErrorMessage", rsp.errormsg())
                    .With("Value", rsp.value());
                it->second.Set(CreatePortoError(portoErrorCode, rsp.errormsg()));
            } else {
                try {
                    int exitStatus = std::stoi(rsp.value());
                    YT_TLOG_DEBUG("Container finished with exit code")
                        .With("Container", container)
                        .With("ExitCode", exitStatus);

                    it->second.Set(exitStatus);
                } catch (const std::exception& ex) {
                    it->second.Set(TError("Failed to parse Porto exit status").With(ex));
                }
            }
        }
        RemoveFromPoller(container);
    }

    void RemoveFromPoller(const std::string& container)
    {
        ContainerMap_.erase(container);

        Containers_.clear();
        for (const auto& [name, pid] : ContainerMap_) {
            Containers_.push_back(name);
        }
    }
};

const std::vector<std::string> TPortoExecutor::ContainerRequestVars_ = {
    "state",
    "exit_status"
};

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(
    TPortoExecutorDynamicConfigPtr config,
    std::string threadNameSuffix,
    const NProfiling::TProfiler& profiler,
    int apiThreadCount)
{
    return New<TPortoExecutor>(
        std::move(config),
        threadNameSuffix,
        profiler,
        apiThreadCount);
}

////////////////////////////////////////////////////////////////////////////////

#else

IPortoExecutorPtr CreatePortoExecutor(
    TPortoExecutorDynamicConfigPtr /*config*/,
    std::string /*threadNameSuffix*/,
    const NProfiling::TProfiler& /*profiler*/,
    int /*apiThreadCount*/)
{
    THROW_ERROR_EXCEPTION("Porto executor is not available on this platform");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
