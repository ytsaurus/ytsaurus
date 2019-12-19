#ifdef __linux__

#include "porto_executor.h"

#include "private.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/ytree/convert.h>

#include <infra/porto/proto/rpc.pb.h>

#include <string>

namespace NYT::NContainers {

using namespace NConcurrency;
using Porto::EError;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

EPortoErrorCode ConvertPortoErrorCode(EError portoError)
{
    return static_cast<EPortoErrorCode>(PortoErrorCodeBase + portoError);
}

std::map<TString, TErrorOr<TString>> ParsePortoGetResponse(
    const TString& name,
    const Porto::TGetResponse& getResponse)
{
    for (const auto& container : getResponse.list()) {
        if (container.name() == name) {
            std::map<TString, TErrorOr<TString>> result;
            for (const auto& property : container.keyval()) {
                if (property.error() == EError::Success) {
                    result[property.variable()] = property.value();
                } else {
                    result[property.variable()] = TError(ConvertPortoErrorCode(property.error()), property.errormsg())
                        << TErrorAttribute("porto_error", ConvertPortoErrorCode(property.error()));
                }
            }

            return result;
        }
    }

    THROW_ERROR_EXCEPTION("Unable to get properties from porto")
        << TErrorAttribute("container", name);
}

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutor
    : public IPortoExecutor
{
public:
    TPortoExecutor(
        const TString& name,
        std::unique_ptr<Porto::TPortoApi> api,
        TDuration retryTime,
        TDuration pollPeriod)
        : Queue_(New<TActionQueue>(Format("Porto:%v", name)))
        , Api_(std::move(api))
        , RetryTime_(retryTime)
    {
        PollExecutor_ = New<TPeriodicExecutor>(
            Queue_->GetInvoker(),
            BIND(&TPortoExecutor::DoPoll, MakeWeak(this)),
            pollPeriod);
        PollExecutor_->Start();
    }

    virtual TFuture<void> CreateContainer(const TString& name) override
    {
        return BIND(&TPortoExecutor::DoCreate, MakeStrong(this), name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> SetProperty(const TString& name, const TString& key, const TString& value) override
    {
        return BIND(&TPortoExecutor::DoSetProperty, MakeStrong(this), name, key, value)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> DestroyContainer(const TString& name) override
    {
        return BIND(&TPortoExecutor::DoDestroy, MakeStrong(this), name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> Stop(const TString& name) override
    {
        return BIND(&TPortoExecutor::DoStop, MakeStrong(this), name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> Start(const TString& name) override
    {
        return BIND(&TPortoExecutor::DoStart, MakeStrong(this), name)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<void> Kill(const TString& name, int signal) override
    {
        return BIND(&TPortoExecutor::DoKill, MakeStrong(this), name, signal)
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListContainers() override
    {
        return BIND(&TPortoExecutor::DoList, MakeStrong(this))
            .AsyncVia(Queue_->GetInvoker())
            .Run();
    }

    virtual TFuture<int> AsyncPoll(const TString& name) override
    {
        return BIND(&TPortoExecutor::AddToPoll, MakeStrong(this), name)
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

    virtual TFuture<std::map<TString, TErrorOr<TString>>> GetProperties(const TString& name, const std::vector<TString>& properties) override
    {
        return BIND([=, this_ = MakeStrong(this)] () {
            const std::vector<TString> containers{name};
            auto getResponse = DoGet(containers, properties);
            return ParsePortoGetResponse(name, getResponse);
        })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    }

    virtual TFuture<TString> CreateVolume(
        const TString& path,
        const std::map<TString, TString>& properties) override
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
    const TActionQueuePtr Queue_;
    const std::unique_ptr<Porto::TPortoApi> Api_;
    const TDuration RetryTime_;

    TPeriodicExecutorPtr PollExecutor_;
    std::vector<TString> Containers_;
    THashMap<TString, TPromise<int>> ContainersMap_;
    TSingleShotCallbackList<void(const TError&)> Failed_;

    static const std::vector<TString> ContainerRequestVars_;

    static TError CreatePortoError(EPortoErrorCode errorCode, const TString& message)
    {
        return TError(errorCode, "Porto API error")
            << TErrorAttribute("original_porto_error_code", static_cast<int>(errorCode) - PortoErrorCodeBase)
            << TErrorAttribute("porto_error_message", message);
    }

    void RetrySleep()
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    }

    void HandleApiErrors(const TString& command, TInstant time)
    {
        TString message;
        auto error = ConvertPortoErrorCode(Api_->GetLastError(message));
        if (error == EPortoErrorCode::ContainerDoesNotExist || error == EPortoErrorCode::InvalidState) {
            // This is typical during job cleanup: we might try to kill a container that is already stopped.
            YT_LOG_DEBUG("Porto API error (Error: %v, Command: %v, Message: %v)",
                error,
                command,
                message);
        } else {
            YT_LOG_ERROR("Porto API error (Error: %v, Command: %v, Message: %v)",
                error,
                command,
                message);
        }

        if (error == EPortoErrorCode::Unknown && TInstant::Now() - time < RetryTime_) {
            return;
        }

        THROW_ERROR_EXCEPTION(CreatePortoError(error, message));
    }

    void DoCreate(const TString& name)
    {
        RunWithRetries([&] () { return Api_->Create(name); }, "Create");
    }

    void DoSetProperty(const TString& name, const TString& key, const TString& value)
    {
        RunWithRetries([&] () { return Api_->SetProperty(name, key, value); }, "SetProperty");
    }

    void DoDestroy(const TString& name)
    {
        try {
            RunWithRetries([&] () { return Api_->Destroy(name); }, "Destroy");
        } catch (const TErrorException& ex) {
            if (!ex.Error().FindMatching(EPortoErrorCode::ContainerDoesNotExist)) {
                throw;
            }
        }
    }

    void DoStop(const TString& name)
    {
        RunWithRetries([&] () { return Api_->Stop(name); }, "Stop");
    }

    void DoStart(const TString& name)
    {
        RunWithRetries([&] () { return Api_->Start(name); }, "Start");
    }

    void DoKill(const TString& name, int signal)
    {
        RunWithRetries([&] () { return Api_->Kill(name, signal); }, "Kill");
    }

    std::vector<TString> DoList()
    {
        TVector<TString> clist;
        RunWithRetries([&] () { return Api_->List(clist); }, "List");

        return std::vector<TString>(clist.begin(), clist.end());
    }

    TFuture<int> AddToPoll(const TString& name)
    {
        auto entry = ContainersMap_.insert({name, NewPromise<int>()});
        if (!entry.second) {
            YT_LOG_WARNING("Container already added for polling (Container: %v)",
                name);
        } else {
            Containers_.push_back(name);
        }
        return entry.first->second.ToFuture();
    }

    Porto::TGetResponse DoGet(
        const std::vector<TString>& containers,
        const std::vector<TString>& vars)
    {
        TVector<TString> containers_(containers.begin(), containers.end());
        TVector<TString> vars_(vars.begin(), vars.end());

        const Porto::TGetResponse* getResponse;

        RunWithRetries([&]() {
            getResponse = Api_->Get(containers_, vars_);
            if (getResponse) {
                return EError::Success;
            } else {
                return EError::Unknown;
            }
        }, "Get");

        YT_VERIFY(getResponse);
        return *getResponse;
    }

    void DoPoll()
    {
        try {
            if (Containers_.empty()) {
                return;
            }

            auto getResponse = DoGet(Containers_, ContainerRequestVars_);

            if (getResponse.list().empty()) {
                return;
            }

            auto getProperty = [](
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
        const std::map<TString, TString>& properties)
    {
        TString volume = path;
        RunWithRetries([&]() {
            return Api_->CreateVolume(volume, TMap<TString, TString>(properties.begin(), properties.end()));
        }, "CreateVolume");

        return volume;
    }

    void DoLinkVolume(const TString& path, const TString& container)
    {
        RunWithRetries([&]() { return Api_->LinkVolume(path, container); }, "LinkVolume");
    }

    void DoUnlinkVolume(const TString& path, const TString& container)
    {
        RunWithRetries([&]() { return Api_->UnlinkVolume(path, container); }, "UnlinkVolume");
    }

    std::vector<TString> DoListVolumePaths()
    {
        TVector<TString> volumes;
        RunWithRetries([&]() { return Api_->ListVolumes(volumes); }, "ListVolume");

        return std::vector<TString>(volumes.begin(), volumes.end());
    }

    void DoImportLayer(const TString& archivePath, const TString& layerId, const TString& place)
    {
        RunWithRetries([&]() { return Api_->ImportLayer(layerId, archivePath, false, place); }, "ImportLayer");
    }

    void DoRemoveLayer(const TString& layerId, const TString& place)
    {
        RunWithRetries([&]() { return Api_->RemoveLayer(layerId, place); }, "RemoveLayer");
    }

    std::vector<TString> DoListLayers(const TString& place)
    {
        TVector<TString> layers;
        RunWithRetries([&]() { return Api_->ListLayers(layers, place); }, "ListLayers");
        return std::vector<TString>(layers.begin(), layers.end());
    }

    void RunWithRetries(std::function<EError()> action, const TString& name) {
        TInstant now = TInstant::Now();
        while (action() != EError::Success) {
            HandleApiErrors(name, now);
            RetrySleep();
        }
    }

    void HandleResult(const TString& name, const Porto::TGetResponse::TContainerGetValueResponse& rsp)
    {
        auto portoErrorCode = ConvertPortoErrorCode(rsp.error());
        auto it = ContainersMap_.find(name);
        if (it == ContainersMap_.end()) {
            YT_LOG_ERROR("Got an unexpected container "
                "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                name,
                portoErrorCode,
                rsp.errormsg(),
                rsp.value());
            return;
        } else {
            if (portoErrorCode != EPortoErrorCode::Success) {
                YT_LOG_ERROR("Container finished with porto API error "
                    "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                    name,
                    portoErrorCode,
                    rsp.errormsg(),
                    rsp.value());
                it->second.Set(CreatePortoError(portoErrorCode, rsp.errormsg()));
            } else {
                try {
                    int exitStatus = std::stoi(rsp.value());
                    YT_LOG_DEBUG("Container finished with exit code (Container: %v, ExitCode: %v)",
                        name,
                        exitStatus);

                    it->second.Set(exitStatus);
                } catch (const std::exception& ex) {
                    it->second.Set(TError("Failed to parse porto exit status") << ex);
                }
            }
        }
        RemoveFromPoller(name);
    }

    void RemoveFromPoller(const TString& name)
    {
        ContainersMap_.erase(name);
        Containers_.clear();

        for (const auto containerIt : ContainersMap_) {
            Containers_.push_back(containerIt.first);
        }
    }
};

const std::vector<TString> TPortoExecutor::ContainerRequestVars_ = {
    "state",
    "exit_status"
};

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(const TString& name, TDuration retryTime, TDuration pollPeriod)
{
    auto api = std::make_unique<Porto::TPortoApi>();
    return New<TPortoExecutor>(name, std::move(api), retryTime, pollPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

#endif
