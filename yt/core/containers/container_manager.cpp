#include "container_manager.h"
#include "instance.h"
#include "porto_executor.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NContainers {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

class TPortoManager
    : public IContainerManager
{
public:
    TPortoManager(
        const Stroka& prefix,
        TCallback<void(const TError&)> errorHandler,
        const TPortoManagerConfig& portoManagerConfig)
        : Prefix_(prefix)
        , ErrorHandler_(errorHandler)
        , PortoManagerConfig_(portoManagerConfig)
    { }

    virtual IInstancePtr CreateInstance() override
    {
        EnsureExecutor();
        return CreatePortoInstance(Prefix_ + '_' + ToString(InstanceId_++), Executor_);
    }

    virtual IInstancePtr GetSelfInstance() override
    {
        EnsureExecutor();
        return GetSelfPortoInstance(Executor_);
    }

    virtual TFuture<std::vector<Stroka>> GetInstanceNames() const override
    {
        EnsureExecutor();
        return Executor_->ListContainers();
    }

    static IContainerManagerPtr Create(
        const Stroka& prefix,
        TCallback<void(const TError&)> errorHandler,
        const TPortoManagerConfig& portoManagerConfig)
    {
        auto manager = New<TPortoManager>(prefix, errorHandler, portoManagerConfig);
        manager->CleanContainers();
        return manager;
    }

private:
    const Stroka Prefix_;
    const TCallback <void(const TError&)> ErrorHandler_;
    const TPortoManagerConfig PortoManagerConfig_;

    std::atomic<ui64> InstanceId_ = {0};
    mutable IPortoExecutorPtr Executor_;

    void EnsureExecutor() const
    {
        if (!Executor_) {
            Executor_ = CreatePortoExecutor(
                PortoManagerConfig_.RetryTime,
                PortoManagerConfig_.PollPeriod);
            Executor_->SubscribeFailed(ErrorHandler_);
        }
    }

    Stroka GetState(const Stroka& name) const
    {
        auto state = WaitFor(Executor_->GetProperties(name, std::vector<Stroka>{"state"}))
            .ValueOrThrow();
        return state.at("state")
            .ValueOrThrow();
    }

    virtual TFuture<void> Destroy(const Stroka& name)
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
        LOG_DEBUG("Cleaning requested (Prefix: %v, Containers: %v)", Prefix_, containers);

        std::vector<TFuture<void>> actions;
        for (const auto& name : containers) {
            if (name == "/") {
                continue;
            }
            if (!name.StartsWith(Prefix_)) {
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
        WaitFor(Combine(actions))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

IContainerManagerPtr CreatePortoManager(
    const Stroka& prefix,
    TCallback<void(const TError&)> errorHandler,
    const TPortoManagerConfig& portoManagerConfig)
{
    return TPortoManager::Create(prefix, errorHandler, portoManagerConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
