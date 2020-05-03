#ifdef __linux__

#include "porto_executor.h"
#include "config.h"

#include "private.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/convert.h>

#include <infra/porto/proto/rpc.pb.h>

#include <string>

namespace NYT::NContainers {

using namespace NConcurrency;
using Porto::EError;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = ContainersLogger;
static constexpr auto RetryInterval = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

EPortoErrorCode ConvertPortoErrorCode(EError portoError)
{
    return static_cast<EPortoErrorCode>(PortoErrorCodeBase + portoError);
}

bool IsRetriableErrorCode(EPortoErrorCode error)
{
    return
        error == EPortoErrorCode::Unknown ||
        error == EPortoErrorCode::SocketError;
}

THashMap<TString, TErrorOr<TString>> ParsePortoGetResponse(
    const Porto::TGetResponse_TContainerGetListResponse& response)
{
    THashMap<TString, TErrorOr<TString>> result;
    for (const auto& property : response.keyval()) {
        if (property.error() == EError::Success) {
            result[property.variable()] = property.value();
        } else {
            result[property.variable()] = TError(ConvertPortoErrorCode(property.error()), property.errormsg())
                << TErrorAttribute("porto_error", ConvertPortoErrorCode(property.error()));
        }
    }
    return result;
}

THashMap<TString, TErrorOr<TString>> ParseSinglePortoGetResponse(
    const TString& name,
    const Porto::TGetResponse& getResponse)
{
    for (const auto& container : getResponse.list()) {
        if (container.name() == name) {
            return ParsePortoGetResponse(container);
        }
    }
    THROW_ERROR_EXCEPTION("Unable to get properties from Porto")
        << TErrorAttribute("container", name);
}

THashMap<TString, THashMap<TString, TErrorOr<TString>>> ParseMultiplePortoGetResponse(
    const Porto::TGetResponse& getResponse)
{
    THashMap<TString, THashMap<TString, TErrorOr<TString>>> result;
    for (const auto& container : getResponse.list()) {
        result[container.name()] = ParsePortoGetResponse(container);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutor
    : public IPortoExecutor
{
public:
    TPortoExecutor(
        TPortoExecutorConfigPtr config,
        const TString& threadNameSuffix,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , Queue_(New<TActionQueue>(Format("Porto:%v", threadNameSuffix)))
        , Profiler_(profiler)
        , PollExecutor_(New<TPeriodicExecutor>(
            Queue_->GetInvoker(),
            BIND(&TPortoExecutor::DoPoll, MakeWeak(this)),
            Config_->PollPeriod))
    {
        Api_->SetTimeout(Config_->ApiTimeout.Seconds());
        Api_->SetDiskTimeout(Config_->ApiDiskTimeout.Seconds());

        PollExecutor_->Start();
    }

    virtual TFuture<void> CreateContainer(const TString& container) override
    {
        return BIND(&TPortoExecutor::DoCreateContainer, MakeStrong(this), container)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<THashMap<TString, TErrorOr<TString>>> GetContainerProperties(
        const TString& container,
        const std::vector<TString>& properties) override
    {
        return BIND([=, this_ = MakeStrong(this)] {
            auto response = DoGetContainerProperties({container}, properties);
            return ParseSinglePortoGetResponse(container, response);
        })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    }

    virtual TFuture<THashMap<TString, THashMap<TString, TErrorOr<TString>>>> GetContainerProperties(
        const std::vector<TString>& containers,
        const std::vector<TString>& properties) override
    {
        return BIND([=, this_ = MakeStrong(this)] {
            auto response = DoGetContainerProperties(containers, properties);
            return ParseMultiplePortoGetResponse(response);
        })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    }

    virtual TFuture<THashMap<TString, i64>> GetContainerMetrics(
        const std::vector<TString>& containers,
        const TString& metric) override
    {
        return BIND([=, this_ = MakeStrong(this)] {
            return DoGetContainerMetrics(containers, metric);
        })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    }

    virtual TFuture<void> SetContainerProperty(
        const TString& container,
        const TString& property,
        const TString& value) override
    {
        return BIND(&TPortoExecutor::DoSetContainerProperty, MakeStrong(this), container, property, value)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> DestroyContainer(const TString& container) override
    {
        return BIND(&TPortoExecutor::DoDestroyContainer, MakeStrong(this), container)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> StopContainer(const TString& container) override
    {
        return BIND(&TPortoExecutor::DoStopContainer, MakeStrong(this), container)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> StartContainer(const TString& container) override
    {
        return BIND(&TPortoExecutor::DoStartContainer, MakeStrong(this), container)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> KillContainer(const TString& container, int signal) override
    {
        return BIND(&TPortoExecutor::DoKillContainer, MakeStrong(this), container, signal)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListContainers() override
    {
        return BIND(&TPortoExecutor::DoListContainers, MakeStrong(this))
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListSubcontainers(
        const TString& rootContainer,
        bool includeRoot) override
    {
        return ListContainers()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const std::vector<TString>& allContainers) {
                return
                    GetContainerProperties(
                        allContainers,
                        std::vector<TString>{"absolute_name"})
                    .Apply(BIND([=, this_ = MakeStrong(this)] (const THashMap<TString, THashMap<TString, TErrorOr<TString>>>& propertyMap) {
                        std::vector<TString> matchingContainers;
                        for (const auto& [someContainer, someProperties] : propertyMap) {
                            if (someContainer == "/") {
                                continue;
                            }

                            const auto& absoluteNameOrError = GetOrCrash(someProperties, "absolute_name");
                            if (!absoluteNameOrError.IsOK()) {
                                continue;
                            }

                            const auto& absoluteName = absoluteNameOrError.Value();
                            if (absoluteName.StartsWith(rootContainer + "/") ||
                                includeRoot && absoluteName == rootContainer)
                            {
                                matchingContainers.push_back(someContainer);
                            }
                        }
                        return matchingContainers;
                    }));
            }));
    }

    virtual TFuture<int> PollContainer(const TString& container) override
    {
        return BIND(&TPortoExecutor::DoPollContainer, MakeStrong(this), container)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual void SubscribeFailed(const TCallback<void (const TError&)>& callback)
    {
        Failed_.Subscribe(callback);
    }

    virtual void UnsubscribeFailed(const TCallback<void (const TError&)>& callback)
    {
        Failed_.Unsubscribe(callback);
    }

    virtual TFuture<TString> CreateVolume(
        const TString& path,
        const THashMap<TString, TString>& properties) override
    {
        return BIND(&TPortoExecutor::DoCreateVolume, MakeStrong(this), path, properties)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> LinkVolume(
        const TString& path,
        const TString& name) override
    {
        return BIND(&TPortoExecutor::DoLinkVolume, MakeStrong(this), path, name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> UnlinkVolume(
        const TString& path,
        const TString& name) override
    {
        return BIND(&TPortoExecutor::DoUnlinkVolume, MakeStrong(this), path, name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListVolumePaths() override
    {
        return BIND(&TPortoExecutor::DoListVolumePaths, MakeStrong(this))
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> ImportLayer(const TString& archivePath, const TString& layerId, const TString& place) override
    {
        return BIND(&TPortoExecutor::DoImportLayer, MakeStrong(this), archivePath, layerId, place)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> RemoveLayer(const TString& layerId, const TString& place) override
    {
        return BIND(&TPortoExecutor::DoRemoveLayer, MakeStrong(this), layerId, place)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListLayers(const TString& place) override
    {
        return BIND(&TPortoExecutor::DoListLayers, MakeStrong(this), place)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

private:
    const TPortoExecutorConfigPtr Config_;
    const TActionQueuePtr Queue_;
    const NProfiling::TProfiler Profiler_;

    const std::unique_ptr<Porto::TPortoApi> Api_ = std::make_unique<Porto::TPortoApi>();
    const TPeriodicExecutorPtr PollExecutor_;

    std::vector<TString> Containers_;
    THashMap<TString, TPromise<int>> ContainerMap_;
    TSingleShotCallbackList<void(const TError&)> Failed_;

    struct TCommandEntry
    {
        explicit TCommandEntry(const NProfiling::TTagIdList& tagIds)
            : TimeGauge("/command_time", tagIds)
            , RetryCounter("/command_retries", tagIds)
            , SuccessCounter("/command_successes", tagIds)
            , FailureCounter("/command_failures", tagIds)
        { }

        NProfiling::TAggregateGauge TimeGauge;
        NProfiling::TMonotonicCounter RetryCounter;
        NProfiling::TMonotonicCounter SuccessCounter;
        NProfiling::TMonotonicCounter FailureCounter;
    };

    TSpinLock CommandLock_;
    THashMap<TString, TCommandEntry> CommandToEntry_;

    static const std::vector<TString> ContainerRequestVars_;

    static TError CreatePortoError(EPortoErrorCode errorCode, const TString& message)
    {
        return TError(errorCode, "Porto API error")
            << TErrorAttribute("original_porto_error_code", static_cast<int>(errorCode) - PortoErrorCodeBase)
            << TErrorAttribute("porto_error_message", message);
    }

    void DoCreateContainer(const TString& container)
    {
        ExecuteApiCall(
            [&] { return Api_->Create(container); },
            "Create");
    }

    void DoSetContainerProperty(const TString& container, const TString& property, const TString& value)
    {
        ExecuteApiCall(
            [&] { return Api_->SetProperty(container, property, value); },
            "SetProperty");
    }

    void DoDestroyContainer(const TString& container)
    {
        try {
            ExecuteApiCall(
                [&] { return Api_->Destroy(container); },
                "Destroy");
        } catch (const TErrorException& ex) {
            if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                throw;
            }
        }
    }

    void DoStopContainer(const TString& container)
    {
        ExecuteApiCall(
            [&] { return Api_->Stop(container); },
            "Stop");
    }

    void DoStartContainer(const TString& container)
    {
        ExecuteApiCall(
            [&] { return Api_->Start(container); },
            "Start");
    }

    void DoKillContainer(const TString& container, int signal)
    {
        ExecuteApiCall(
            [&] { return Api_->Kill(container, signal); },
            "Kill");
    }

    std::vector<TString> DoListContainers()
    {
        TVector<TString> containers;
        ExecuteApiCall(
            [&] { return Api_->List(containers); }, 
            "List");
        return {containers.begin(), containers.end()};
    }

    TFuture<int> DoPollContainer(const TString& container)
    {
        auto entry = ContainerMap_.insert({container, NewPromise<int>()});
        if (!entry.second) {
            YT_LOG_WARNING("Container already added for polling (Container: %v)",
                container);
        } else {
            Containers_.push_back(container);
        }
        return entry.first->second.ToFuture();
    }

    Porto::TGetResponse DoGetContainerProperties(
        const std::vector<TString>& containers,
        const std::vector<TString>& vars)
    {
        TVector<TString> containers_(containers.begin(), containers.end());
        TVector<TString> vars_(vars.begin(), vars.end());

        const Porto::TGetResponse* getResponse;

        ExecuteApiCall(
            [&] {
                getResponse = Api_->Get(containers_, vars_);
                return getResponse ? EError::Success : EError::Unknown;
            },
            "Get");

        YT_VERIFY(getResponse);
        return *getResponse;
    }

    THashMap<TString, i64> DoGetContainerMetrics(
        const std::vector<TString>& containers,
        const TString& metric)
    {
        TVector<TString> containers_(containers.begin(), containers.end());
        
        TMap<TString, ui64> result;

        ExecuteApiCall(
            [&] { return Api_->GetProcMetric(containers_, metric, result); },
            "GetProcMetric");

        return {result.begin(), result.end()};
    }

    void DoPoll()
    {
        try {
            if (Containers_.empty()) {
                return;
            }

            auto getResponse = DoGetContainerProperties(Containers_, ContainerRequestVars_);

            if (getResponse.list().empty()) {
                return;
            }

            auto getProperty = [] (
                const Porto::TGetResponse::TContainerGetListResponse& container,
                const TString& name) -> Porto::TGetResponse::TContainerGetValueResponse
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
                //TODO(dcherednik): other states
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Fatal exception during porto polling");
            Failed_.Fire(TError(ex));
        }
    }

    TString DoCreateVolume(
        const TString& path,
        const THashMap<TString, TString>& properties)
    {
        auto volume = path;
        TMap<TString, TString> propertyMap(properties.begin(), properties.end());
        ExecuteApiCall(
            [&] { return Api_->CreateVolume(volume, propertyMap); },
            "CreateVolume");
        return volume;
    }

    void DoLinkVolume(const TString& path, const TString& container)
    {
        ExecuteApiCall(
            [&] { return Api_->LinkVolume(path, container); },
            "LinkVolume");
    }

    void DoUnlinkVolume(const TString& path, const TString& container)
    {
        ExecuteApiCall(
            [&] { return Api_->UnlinkVolume(path, container); },
            "UnlinkVolume");
    }

    std::vector<TString> DoListVolumePaths()
    {
        TVector<TString> volumes;
        ExecuteApiCall(
            [&] { return Api_->ListVolumes(volumes); },
            "ListVolume");
        return {volumes.begin(), volumes.end()};
    }

    void DoImportLayer(const TString& archivePath, const TString& layerId, const TString& place)
    {
        ExecuteApiCall(
            [&] { return Api_->ImportLayer(layerId, archivePath, false, place); },
            "ImportLayer");
    }

    void DoRemoveLayer(const TString& layerId, const TString& place)
    {
        ExecuteApiCall(
            [&] { return Api_->RemoveLayer(layerId, place); },
            "RemoveLayer");
    }

    std::vector<TString> DoListLayers(const TString& place)
    {
        TVector<TString> layers;
        ExecuteApiCall(
            [&] { return Api_->ListLayers(layers, place); },
            "ListLayers");
        return {layers.begin(), layers.end()};
    }

    TCommandEntry* GetCommandEntry(const TString& command)
    {
        auto guard = Guard(CommandLock_);
        if (auto it = CommandToEntry_.find(command)) {
            return &it->second;
        }
        NProfiling::TTagIdList tagIds{
            NProfiling::TProfileManager::Get()->RegisterTag("command", command)
        };
        return &CommandToEntry_.emplace(command, TCommandEntry(tagIds)).first->second;
    }

    void ExecuteApiCall(std::function<EError()> callback, const TString& command)
    {
        YT_LOG_DEBUG("Porto API call started (Command: %v)", command);

        auto* entry = GetCommandEntry(command);
        auto startTime = NProfiling::GetInstant();
        while (true) {
            EError error;
            {
                NProfiling::TWallTimer timer;
                error = callback();
                Profiler_.Update(entry->TimeGauge, timer.GetElapsedValue());
            }

            if (error == EError::Success) {
                Profiler_.Increment(entry->SuccessCounter);
                return;
            }

            Profiler_.Increment(entry->FailureCounter);

            HandleApiError(command, startTime);

            YT_LOG_DEBUG("Sleeping and retrying Porto API call (Command: %v)", command);
            Profiler_.Increment(entry->RetryCounter);

            TDelayedExecutor::WaitForDuration(RetryInterval);
        }

        YT_LOG_DEBUG("Porto API call completed (Command: %v)", command);
    }

    void HandleApiError(const TString& command, TInstant startTime)
    {
        TString errorMessage;
        auto error = ConvertPortoErrorCode(Api_->GetLastError(errorMessage));

        // These errors are typical during job cleanup: we might try to kill a container that is already stopped.
        bool debug = (error == EPortoErrorCode::ContainerDoesNotExist || error == EPortoErrorCode::InvalidState);
        YT_LOG_EVENT(
            Logger,
            debug ? NLogging::ELogLevel::Debug : NLogging::ELogLevel::Error,
            "Porto API call error (Error: %v, Command: %v, Message: %v)",
            error,
            command,
            errorMessage);

        if (!IsRetriableErrorCode(error) || NProfiling::GetInstant() - startTime > Config_->RetriesTimeout) {
            THROW_ERROR CreatePortoError(error, errorMessage);
        }
    }

    void HandleResult(const TString& container, const Porto::TGetResponse::TContainerGetValueResponse& rsp)
    {
        auto portoErrorCode = ConvertPortoErrorCode(rsp.error());
        auto it = ContainerMap_.find(container);
        if (it == ContainerMap_.end()) {
            YT_LOG_ERROR("Got an unexpected container "
                "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                container,
                portoErrorCode,
                rsp.errormsg(),
                rsp.value());
            return;
        } else {
            if (portoErrorCode != EPortoErrorCode::Success) {
                YT_LOG_ERROR("Container finished with porto API error "
                    "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                    container,
                    portoErrorCode,
                    rsp.errormsg(),
                    rsp.value());
                it->second.Set(CreatePortoError(portoErrorCode, rsp.errormsg()));
            } else {
                try {
                    int exitStatus = std::stoi(rsp.value());
                    YT_LOG_DEBUG("Container finished with exit code (Container: %v, ExitCode: %v)",
                        container,
                        exitStatus);

                    it->second.Set(exitStatus);
                } catch (const std::exception& ex) {
                    it->second.Set(TError("Failed to parse porto exit status") << ex);
                }
            }
        }
        RemoveFromPoller(container);
    }

    void RemoveFromPoller(const TString& container)
    {
        ContainerMap_.erase(container);

        Containers_.clear();
        for (const auto containerIt : ContainerMap_) {
            Containers_.push_back(containerIt.first);
        }
    }
};

const std::vector<TString> TPortoExecutor::ContainerRequestVars_ = {
    "state",
    "exit_status"
};

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(
    TPortoExecutorConfigPtr config,
    const TString& threadNameSuffix,
    const NProfiling::TProfiler& profiler)
{
    return New<TPortoExecutor>(
        std::move(config),
        threadNameSuffix,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

#endif
