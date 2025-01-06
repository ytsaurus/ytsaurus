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

namespace NYT::NDiskManager {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DiskManagerLogger;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class THotswapManager
    : public IHotswapManager
{
public:
    explicit THotswapManager(const THotswapManagerConfigPtr& config)
        // TODO(babenko): think of a better choice.
        : Invoker_(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , CheckerExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&THotswapManager::OnDiskChangeCheck, MakeWeak(this)),
            TDuration::Minutes(1)))
        , OrchidService_(IYPathService::FromProducer(
            // NB: This is a cyclic reference but it's fine since THotswapManagerImpl is a singleton.
            BIND(&THotswapManager::BuildOrchid, MakeStrong(this))))
        , DiskManagerProxy_(CreateDiskManagerProxy(config->DiskManagerProxy))
        , DiskInfoProvider_(CreateDiskInfoProvider(
            DiskManagerProxy_,
            config->DiskInfoProvider))
    {
        CheckerExecutor_->Start();
    }

    void Reconfigure(const THotswapManagerDynamicConfigPtr& dynamicConfig) final
    {
        DiskManagerProxy_->Reconfigure(dynamicConfig->DiskManagerProxy);
    }

    IYPathServicePtr GetOrchidService() final
    {
        return OrchidService_;
    }

    IDiskInfoProviderPtr GetDiskInfoProvider() final
    {
        return DiskInfoProvider_;
    }

    void PopulateAlerts(std::vector<TError>* alerts) final
    {
        auto alert = DiskIdsMismatchedAlert_.Load();
        if (!alert.IsOK()) {
            alerts->push_back(alert);
        }
    }

private:
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr CheckerExecutor_;
    const NYTree::IYPathServicePtr OrchidService_;
    const IDiskManagerProxyPtr DiskManagerProxy_;
    const IDiskInfoProviderPtr DiskInfoProvider_;

    NThreading::TAtomicObject<TError> DiskIdsMismatchedAlert_;
    THashSet<std::string> OldDiskIds_;

    void BuildOrchid(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("disk_ids_mismatched").Value(!DiskIdsMismatchedAlert_.Load().IsOK())
            .EndMap();
    }

    void OnDiskChangeCheck()
    {
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

IHotswapManagerPtr CreateHotswapManager(THotswapManagerConfigPtr config)
{
    return New<THotswapManager>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
