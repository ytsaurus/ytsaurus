#ifdef __linux__

#include "porto_executor.h"

#include "private.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/ytree/convert.h>

#include <yt/contrib/portoapi/rpc.pb.h>

namespace NYT {
namespace NContainers {

using namespace NConcurrency;
using ::rpc::EError;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

using TPortoResult = std::map<TString, std::map<TString, Porto::GetResponse>>;

static std::map<TString, TErrorOr<TString>> ParsePortoResult(
    const TString& name,
    const TPortoResult& portoResult)
{
    std::map<TString, TErrorOr<TString>> result;
    for (const auto& portoProperty : portoResult.at(name)) {
        if (portoProperty.second.Error == 0) {
            result[portoProperty.first] = portoProperty.second.Value;
        } else {
            result[portoProperty.first] = TError(ContainerErrorCodeBase + portoProperty.second.Error, portoProperty.second.ErrorMsg)
                << TErrorAttribute("porto_error", portoProperty.second.Error);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutor
    : public IPortoExecutor
{
public:
    TPortoExecutor(
        std::unique_ptr<Porto::Connection> api,
        TDuration retryTime,
        TDuration pollPeriod)
        : Api_(std::move(api))
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
            TPortoResult portoResult;
            const std::vector<TString> containers{name};
            DoGet(containers, properties, portoResult);
            if (portoResult.empty() || portoResult.at(name).empty()) {
                THROW_ERROR_EXCEPTION("Unable to get %v properties from porto", properties.size())
                    << TErrorAttribute("container", name)
                    << TErrorAttribute("properties", properties);
            }
            return ParsePortoResult(name, portoResult);
        })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    }

    virtual TFuture<TVolumeId> CreateVolume(
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

    virtual TFuture<std::vector<Porto::Volume>> ListVolumes() override
    {
        return BIND(&TPortoExecutor::DoListVolumes, MakeStrong(this))
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
    const std::unique_ptr<Porto::Connection> Api_;
    const TDuration RetryTime_;

    const TActionQueuePtr Queue_ = New<TActionQueue>("PortoQueue");
    TPeriodicExecutorPtr PollExecutor_;
    std::vector<TString> Containers_;
    THashMap<TString, TPromise<int>> ContainersMap_;
    TSingleShotCallbackList<void(const TError&)> Failed_;

    static const std::vector<TString> ContainerRequestVars_;

    static TError ConvertPortoError(int errorCode, const TString& message)
    {
        return TError(errorCode + ContainerErrorCodeBase, "Porto API error")
            << TErrorAttribute("original_porto_error_code", errorCode)
            << TErrorAttribute("porto_error_message", message);
    }

    void RetrySleep()
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    }

    void HandleApiErrors(const TString& command, TInstant time)
    {
        int error;
        TString message;
        Api_->GetLastError(error, message);
        if (error == EError::ContainerDoesNotExist || error == EError::InvalidState) {
            // This is typical during job cleanup: we might try to kill a container that is already stopped.
            LOG_DEBUG("Porto API error (Error: %v, Command: %v, Message: %v)",
                error,
                command,
                message);
        } else {
            LOG_ERROR("Porto API error (Error: %v, Command: %v, Message: %v)",
                error,
                command,
                message);
        }

        if (error == EError::Unknown && TInstant::Now() - time < RetryTime_) {
            return;
        }

        THROW_ERROR_EXCEPTION(ConvertPortoError(error, message));
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
        RunWithRetries([&] () { return Api_->Destroy(name); }, "Destroy");
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
        std::vector<TString> clist;
        RunWithRetries([&] () { return Api_->List(clist); }, "List");
        return clist;
    }

    TFuture<int> AddToPoll(const TString& name)
    {
        auto entry = ContainersMap_.insert({name, NewPromise<int>()});
        if (!entry.second) {
            LOG_WARNING("Container already added for polling (Container: %v)",
                name);
        } else {
            Containers_.push_back(name);
        }
        return entry.first->second.ToFuture();
    }

    void DoGet(
        const std::vector<TString>& containers,
        const std::vector<TString>& vars,
        std::map<TString, std::map<TString, Porto::GetResponse>>& result)
    {
        RunWithRetries([&]() { return Api_->Get(containers, vars, result); }, "Get");
    }

    void DoPoll()
    {
        std::map<TString, std::map<TString, Porto::GetResponse>> pollResults;
        try {
            pollResults.clear();
            if (Containers_.empty()) {
                return;
            }

            DoGet(Containers_, ContainerRequestVars_, pollResults);

            if (pollResults.empty()) {
                return;
            }

            for (auto& container: pollResults) {
                auto state = container.second["state"];
                // Someone destroyed container before us.
                if (state.Error == EError::ContainerDoesNotExist) {
                    HandleResult(container.first, state);
                    continue;
                }
                if (state.Value == "dead") {
                    HandleResult(container.first, container.second["exit_status"]);
                }
                //TODO(dcherednik): other states
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Fatal exception during porto polling");
            Failed_.Fire(TError(ex));
        }
    }

    TVolumeId DoCreateVolume(
        const TString& path,
        const std::map<TString, TString>& properties)
    {
        Porto::Volume volume;
        RunWithRetries([&]() { return Api_->CreateVolume(path, properties, volume); }, "CreateVolume");
        return { volume.Path };
    }

    void DoLinkVolume(const TString& path, const TString& container)
    {
        RunWithRetries([&]() { return Api_->LinkVolume(path, container); }, "LinkVolume");
    }

    void DoUnlinkVolume(const TString& path, const TString& container)
    {
        RunWithRetries([&]() { return Api_->UnlinkVolume(path, container); }, "UnlinkVolume");
    }

    std::vector<Porto::Volume> DoListVolumes()
    {
        std::vector<Porto::Volume> volumes;
        RunWithRetries([&]() { return Api_->ListVolumes(volumes); }, "ListVolume");
        return volumes;
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
        std::vector<TString> layers;
        RunWithRetries([&]() { return Api_->ListLayers(layers, place); }, "ListLayers");
        return layers;
    }

    void RunWithRetries(std::function<bool()> action, const TString& name) {
        TInstant now = TInstant::Now();
        while (action()) {
            HandleApiErrors(name, now);
            RetrySleep();
        }
    }

    void HandleResult(const TString& name, const Porto::GetResponse& rsp)
    {
        auto it = ContainersMap_.find(name);
        if (it == ContainersMap_.end()) {
            LOG_ERROR("Got an unexpected container "
                "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                name,
                rsp.Error,
                rsp.ErrorMsg,
                rsp.Value);
            return;
        } else {
            if (rsp.Error) {
                LOG_ERROR("Container finished with porto API error "
                    "(Container: %v, ResponseError: %v, ErrorMessage: %v, Value: %v)",
                    name,
                    rsp.Error,
                    rsp.ErrorMsg,
                    rsp.Value);
                it->second.Set(ConvertPortoError(rsp.Error, rsp.ErrorMsg));
            } else {
                try {
                    int exitStatus = std::stoi(rsp.Value);
                    LOG_DEBUG("Container finished with exit code (Container: %v, ExitCode: %v)",
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

IPortoExecutorPtr CreatePortoExecutor(TDuration retryTime, TDuration pollPeriod)
{
    std::unique_ptr<Porto::Connection> api(new Porto::Connection);
    return New<TPortoExecutor>(std::move(api), retryTime, pollPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT

#endif
