#include "hotswap_manager.h"

#include "private.h"
#include "config.h"
#include "disk_info_provider.h"
#include "disk_manager_proxy.h"

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NDiskManager {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DiskManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TUnconfiguredDiskInfoProvider
    : public IDiskInfoProvider
{
public:
    static IDiskInfoProviderPtr Get()
    {
        return LeakyRefCountedSingleton<TUnconfiguredDiskInfoProvider>();
    }

    const std::vector<std::string>& GetConfigDiskIds() final
    {
        static const std::vector<std::string> empty;
        return empty;
    }

    TFuture<std::vector<TDiskInfo>> GetYTDiskInfos() final
    {
        return MakeFuture<std::vector<TDiskInfo>>({});
    }

    TFuture<void> UpdateDiskCache() final
    {
        return VoidFuture;
    }

    TFuture<void> RecoverDisk(const std::string& /*diskId*/) final
    {
        return MakeFuture<void>(TError("Cannot recover disk: hotswap dispatcher is not configured"));
    }

    TFuture<void> FailDisk(
        const std::string& /*diskId*/,
        const std::string& /*reason*/) final
    {
        return MakeFuture<void>(TError("Cannot fail disk: hotswap dispatcher is not configured"));
    }

    TFuture<bool> GetHotSwapEnabled() final
    {
        return FalseFuture;
    }

private:
    TUnconfiguredDiskInfoProvider() = default;

    DECLARE_LEAKY_REF_COUNTED_SINGLETON_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

class THotswapManagerImpl
    : public TRefCounted
{
public:
    static THotswapManagerImpl* Get()
    {
        return LeakyRefCountedSingleton<THotswapManagerImpl>().Get();
    }

    void Configure(const THotswapManagerConfigPtr& config)
    {
        auto guard = Guard(ConfigLock_);

        if (std::exchange(Configured_, true)) {
            THROW_ERROR_EXCEPTION("Hotswap dispatcher is already configured");
        }

        DiskManagerProxy_ = CreateDiskManagerProxy(config->DiskManagerProxy);
        DiskInfoProvider_ = CreateDiskInfoProvider(
            DiskManagerProxy_,
            config->DiskInfoProvider);
        CheckerExecutor_->Start();

        YT_LOG_INFO("Hotswap dispatcher configured");
    }

    void Reconfigure(const THotswapManagerDynamicConfigPtr& dynamicConfig)
    {
        auto guard = Guard(ConfigLock_);

        if (!Configured_) {
            THROW_ERROR_EXCEPTION("Hotswap dispatcher is not configured yet");
        }

        DiskManagerProxy_->Reconfigure(dynamicConfig->DiskManagerProxy);

        YT_LOG_INFO("Hotswap dispatcher reconfigured");
    }


    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }

    IDiskInfoProviderPtr GetDiskInfoProvider()
    {
        auto guard = Guard(ConfigLock_);
        return Configured_ ? DiskInfoProvider_ : TUnconfiguredDiskInfoProvider::Get();
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        auto alert = DiskIdsMismatchedAlert_.Load();
        if (!alert.IsOK()) {
            alerts->push_back(alert);
        }
    }

private:
    DECLARE_LEAKY_REF_COUNTED_SINGLETON_FRIEND();

    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr CheckerExecutor_;
    const NYTree::IYPathServicePtr OrchidService_;

    NThreading::TSpinLock ConfigLock_;
    bool Configured_ = false;
    IDiskManagerProxyPtr DiskManagerProxy_;
    IDiskInfoProviderPtr DiskInfoProvider_;

    NThreading::TAtomicObject<TError> DiskIdsMismatchedAlert_;
    THashSet<std::string> OldDiskIds_;

    DECLARE_THREAD_AFFINITY_SLOT(CheckThread);


    THotswapManagerImpl()
        // TODO(babenko): think of a better choice.
        : Invoker_(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , CheckerExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&THotswapManagerImpl::OnDiskChangeCheck, MakeWeak(this)),
            TDuration::Minutes(1)))
        , OrchidService_(IYPathService::FromProducer(
            // NB: This is a cyclic reference but it's fine since THotswapManagerImpl is a singleton.
            BIND(&THotswapManagerImpl::BuildOrchid, MakeStrong(this))))
    { }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("disk_ids_mismatched").Value(!DiskIdsMismatchedAlert_.Load().IsOK())
            .EndMap();
    }

    void OnDiskChangeCheck()
    {
        VERIFY_THREAD_AFFINITY(CheckThread);

        auto diskInfosOrError = WaitFor(DiskInfoProvider_->GetYTDiskInfos());

        // Fast path.
        if (!diskInfosOrError.IsOK()) {
            YT_LOG_EVENT(
                Logger,
                diskInfosOrError.FindMatching(NRpc::EErrorCode::NoSuchService) ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Info,
                diskInfosOrError,
                "Failed to list disk infos");
            return;
        }

        auto oldDiskIds = OldDiskIds_;

        THashSet<std::string> diskIds;
        THashSet<std::string> aliveDiskIds;
        THashSet<std::string> configDiskIds;

        const auto& diskInfos = diskInfosOrError.Value();
        for (const auto& diskInfo : diskInfos) {
            if (diskInfo.State == EDiskState::OK) {
                aliveDiskIds.insert(diskInfo.DiskId);
                diskIds.insert(diskInfo.DiskId);
            }
        }

        for (const auto& diskId : DiskInfoProvider_->GetConfigDiskIds()) {
            configDiskIds.insert(diskId);
        }

        auto checkDisks = [] (const THashSet<std::string>& oldDiskIds, const THashSet<std::string>& newDiskIds) {
            for (const auto& newDiskId : newDiskIds) {
                if (!oldDiskIds.contains(newDiskId)) {
                    return false;
                }
            }

            return true;
        };

        if (!oldDiskIds.empty() && !configDiskIds.empty()) {
            if (!checkDisks(oldDiskIds, aliveDiskIds) ||
                !checkDisks(aliveDiskIds, oldDiskIds) ||
                !checkDisks(configDiskIds, diskIds) ||
                !checkDisks(diskIds, configDiskIds))
            {
                YT_LOG_WARNING("Mismatching disk ids found");
                DiskIdsMismatchedAlert_.Store(TError(NDiskManager::EErrorCode::DiskIdsMismatched, "Disk ids mismatched")
                    << TErrorAttribute("config_disk_ids", std::vector(configDiskIds.begin(), configDiskIds.end()))
                    << TErrorAttribute("disk_ids", std::vector(diskIds.begin(), diskIds.end()))
                    << TErrorAttribute("previous_alive_disk_ids", std::vector(oldDiskIds.begin(), oldDiskIds.end()))
                    << TErrorAttribute("alive_disk_ids", std::vector(aliveDiskIds.begin(), aliveDiskIds.end())));
            }
        }

        OldDiskIds_ = std::move(aliveDiskIds);
    }
};

////////////////////////////////////////////////////////////////////////////////

void THotswapManager::Configure(const THotswapManagerConfigPtr& config)
{
    THotswapManagerImpl::Get()->Configure(config);
}

void THotswapManager::Reconfigure(const THotswapManagerDynamicConfigPtr& dynamicConfig)
{
    THotswapManagerImpl::Get()->Reconfigure(dynamicConfig);
}

IDiskInfoProviderPtr THotswapManager::GetDiskInfoProvider()
{
    return THotswapManagerImpl::Get()->GetDiskInfoProvider();
}

void THotswapManager::PopulateAlerts(std::vector<TError>* alerts)
{
    THotswapManagerImpl::Get()->PopulateAlerts(alerts);
}

IYPathServicePtr THotswapManager::GetOrchidService()
{
    return THotswapManagerImpl::Get()->GetOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
