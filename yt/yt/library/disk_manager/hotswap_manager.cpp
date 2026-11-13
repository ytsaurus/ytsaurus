#include "hotswap_manager.h"

#include "private.h"
#include "config.h"
#include "disk_info_provider.h"
#include "disk_manager_proxy.h"

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/system/atomic_object.h>

#include <algorithm>

namespace NYT::NDiskManager {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DiskManagerLogger;

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
        , DiskScope_(config->DiskScope)
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
    const TDiskScopeConfigPtr DiskScope_;

    TAtomicObject<TError> DiskIdsMismatchedAlert_;
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
            YT_TLOG_EVENT(
                Logger,
                diskInfosOrError.FindMatching(NRpc::EErrorCode::NoSuchService) ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Info,
                "Failed to list disk infos")
                .With(diskInfosOrError);
            return;
        }

        auto oldDiskIds = OldDiskIds_;

        const bool hasDiskScope = static_cast<bool>(DiskScope_);

        auto isDiskInScope = [&] (const TDiskInfo& diskInfo) {
            if (!hasDiskScope) {
                return true;
            }

            return std::ranges::any_of(diskInfo.PartitionFsLabels, [&] (const auto& fsLabel) {
                return DiskScope_->FsLabels.contains(fsLabel);
            });
        };

        THashSet<std::string> diskIds;
        THashSet<std::string> aliveDiskIds;

        const auto& diskInfos = diskInfosOrError.Value();
        for (const auto& diskInfo : diskInfos) {
            if (diskInfo.State != EDiskState::OK) {
                continue;
            }

            diskIds.insert(diskInfo.DiskId);
            if (isDiskInScope(diskInfo)) {
                aliveDiskIds.insert(diskInfo.DiskId);
            }
        }

        const auto& configDiskIds = hasDiskScope
            ? DiskScope_->DiskIds
            : DiskInfoProvider_->GetConfigDiskIds();

        // Compare disk sets if a scope is configured or both the configured
        // and previous runtime snapshots are non-empty.
        // In scoped mode, report a mismatch if disks appear in the scope
        // but their IDs are missing from the configured snapshot.
        const bool shouldCheckDiskSets = hasDiskScope || (!oldDiskIds.empty() && !configDiskIds.empty());

        // Skip runtime-change detection until a non-empty disk snapshot has been observed.
        const bool runtimeDiskSetChanged = !oldDiskIds.empty() && oldDiskIds != aliveDiskIds;

        const bool configDiskSetMismatched = configDiskIds != aliveDiskIds;

        if (shouldCheckDiskSets && (runtimeDiskSetChanged || configDiskSetMismatched)) {
            YT_TLOG_WARNING("Mismatching disk ids found")
                .With("RuntimeDiskSetChanged", runtimeDiskSetChanged)
                .With("ConfigDiskSetMismatched", configDiskSetMismatched);
            DiskIdsMismatchedAlert_.Store(TError(NDiskManager::EErrorCode::DiskIdsMismatched, "Disk ids mismatched")
                .With("config_disk_ids", std::vector(configDiskIds.begin(), configDiskIds.end()))
                .With("disk_ids", std::vector(diskIds.begin(), diskIds.end()))
                .With("previous_alive_disk_ids", std::vector(oldDiskIds.begin(), oldDiskIds.end()))
                .With("alive_disk_ids", std::vector(aliveDiskIds.begin(), aliveDiskIds.end()))
                .With("disk_scope_enabled", hasDiskScope)
                .With("scoped_disk_fs_labels", hasDiskScope
                    ? std::vector(DiskScope_->FsLabels.begin(), DiskScope_->FsLabels.end())
                    : std::vector<std::string>())
                .With("runtime_disk_set_changed", runtimeDiskSetChanged)
                .With("config_disk_set_mismatched", configDiskSetMismatched));
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
