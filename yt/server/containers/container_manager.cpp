#ifdef __linux__

#include "container_manager.h"
#include "instance.h"
#include "private.h"

#include "porto_executor.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/logging/log.h>

#include <yt/contrib/portoapi/rpc.pb.h>

namespace NYT {
namespace NContainers {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

static TString GetSelfAbsoluteName(IPortoExecutorPtr executor)
{
    auto properties = WaitFor(executor->GetProperties(
        "self",
        std::vector<TString>{"absolute_name"}))
            .ValueOrThrow();

    auto absoluteName = properties.at("absolute_name")
        .ValueOrThrow();

    return absoluteName + "/";
}

////////////////////////////////////////////////////////////////////////////////

class TPortoManager
    : public IContainerManager
{
public:
    virtual IInstancePtr CreateInstance(bool autoDestroy) override
    {
        return CreatePortoInstance(
            BaseName_ + Prefix_ + '_' + ToString(InstanceId_++),
            Executor_,
            autoDestroy);
    }

    virtual IInstancePtr GetSelfInstance() override
    {
        return GetSelfPortoInstance(Executor_);
    }

    virtual IInstancePtr GetInstance(const TString& name) override
    {
        return GetPortoInstance(Executor_, name);
    }

    virtual TFuture<std::vector<TString>> GetInstanceNames() const override
    {
        return Executor_->ListContainers();
    }

    static IContainerManagerPtr Create(
        const TString& prefix,
        const TNullable<TString>& rootContainer,
        TCallback<void(const TError&)> errorHandler,
        const TPortoManagerConfig& portoManagerConfig)
    {
        auto executor = CreatePortoExecutor(
            portoManagerConfig.RetryTime,
            portoManagerConfig.PollPeriod);
        executor->SubscribeFailed(errorHandler);

        auto getRootContainer = [&] () {
            if (rootContainer) {
                // Name of root container must end with "/".
                return *rootContainer + (rootContainer->EndsWith('/') ? "" : "/");
            } else {
                return GetSelfAbsoluteName(executor);
            }
        };

        auto manager = New<TPortoManager>(
            prefix,
            getRootContainer(),
            portoManagerConfig,
            executor);
        manager->CleanContainers();
        return manager;
    }

private:
    const TString Prefix_;
    const TString BaseName_;
    const TPortoManagerConfig PortoManagerConfig_;

    mutable IPortoExecutorPtr Executor_;
    std::atomic<ui64> InstanceId_ = {0};

    TPortoManager(
        const TString& prefix,
        const TString& baseName,
        const TPortoManagerConfig& portoManagerConfig,
        IPortoExecutorPtr executor)
        : Prefix_(prefix)
        , BaseName_(baseName)
        , PortoManagerConfig_(portoManagerConfig)
        , Executor_(executor)
    {
        LOG_DEBUG("Porto manager initialized (Prefix: %v, BaseName: %v)",
            Prefix_,
            BaseName_);
    }

    TString GetState(const TString& name) const
    {
        auto state = WaitFor(Executor_->GetProperties(name, std::vector<TString>{"state"}))
            .ValueOrThrow();
        return state.at("state")
            .ValueOrThrow();
    }

    virtual TFuture<void> Destroy(const TString& name)
    {
        return Executor_->DestroyContainer(name);
    }

    void CleanContainers()
    {
        if (PortoManagerConfig_.CleanMode == ECleanMode::None) {
            return;
        }

        const auto containers = WaitFor(GetInstanceNames())
            .ValueOrThrow();
        LOG_DEBUG("Cleaning requested (Prefix: %v, Containers: %v, BaseName: %v)",
            Prefix_,
            containers,
            BaseName_);

        std::vector<TFuture<void>> actions;
        for (const auto& name : containers) {
            if (name == "/") {
                continue;
            }
            // ToDo(psushin) : fix this mess.
            if (!name.StartsWith(BaseName_ + Prefix_) && !name.StartsWith(Prefix_)) {
                continue;
            }

            if (PortoManagerConfig_.CleanMode == ECleanMode::Dead) {
                auto state = GetState(name);
                if (state != "dead") {
                    continue;
                }
            }
            LOG_DEBUG("Cleaning (Container: %v)", name);
            actions.push_back(Destroy(name));
        }
        auto errors = WaitFor(CombineAll(actions));
        THROW_ERROR_EXCEPTION_IF_FAILED(errors, "Failed to clean containers");

        for (const auto& error : errors.Value()) {
            if (error.IsOK() ||
                error.FindMatching(ContainerErrorCodeBase + ::rpc::EError::ContainerDoesNotExist))
            {
                continue;
            }

            THROW_ERROR_EXCEPTION("Failed to clean containers")
                << error;
        }
    }

    DECLARE_NEW_FRIEND();
};

IContainerManagerPtr CreatePortoManager(
    const TString& prefix,
    const TNullable<TString>& rootContainer,
    TCallback<void(const TError&)> errorHandler,
    const TPortoManagerConfig& portoManagerConfig)
{
    return TPortoManager::Create(prefix, rootContainer, errorHandler, portoManagerConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT

#endif
