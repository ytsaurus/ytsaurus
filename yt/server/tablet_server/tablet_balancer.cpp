#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_balancer.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/public.h>
#include <yt/server/cell_master/world_initializer.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/profiling/profiler.h>

#include <queue>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NTabletNode;
using namespace NTabletServer::NProto;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

// Formatter for TMemoryUsage.
template <class T, class U>
TString ToString(const std::pair<T, U>& pair)
{
    return Format("(%v, %v)", pair.first, pair.second);
}

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletBalancerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , BalanceExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
            BIND(&TImpl::Balance, MakeWeak(this)),
            Config_->BalancePeriod))
        , EnabledCheckExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
            BIND(&TImpl::OnCheckEnabled, MakeWeak(this)),
            Config_->EnabledCheckPeriod))
        , Profiler("/tablet_server/tablet_balancer")
        , MemoryMoveCounter_("/in_memory_moves")
        , MergeCounter_("/tablet_merges")
        , SplitCounter_("/tablet_splits")
        , QueueSizeCounter_("/queue_size")
    { }

    void Start()
    {
        BalanceExecutor_->Start();
        EnabledCheckExecutor_->Start();
        Started_ = true;
    }

    void Stop()
    {
        EnabledCheckExecutor_->Stop();
        BalanceExecutor_->Stop();
        Started_ = false;
    }

    void OnTabletHeartbeat(TTablet* tablet)
    {
        if (!Enabled_) {
            return;
        }

        if (!Started_) {
            return;
        }

        if (!Config_->EnableTabletSizeBalancer) {
            return;
        }

        if (!IsObjectAlive(tablet) ||
            !IsObjectAlive(tablet->GetTable()) ||
            tablet->GetAction() ||
            QueuedTabletIds_.find(tablet->GetId()) != QueuedTabletIds_.end() ||
            !tablet->Replicas().empty())
        {
            return;
        }

        auto size = GetTabletSize(tablet);
        auto bounds = GetTabletSizeConfig(tablet);
        if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
            TabletIdQueue_.push_back(tablet->GetId());
            QueuedTabletIds_.insert(tablet->GetId());
            Profiler.Increment(QueueSizeCounter_);
            LOG_DEBUG("Put tablet %v into balancer queue", tablet->GetId());
        }
    }

private:
    struct TTabletSizeConfig
    {
        i64 MinTabletSize;
        i64 MaxTabletSize;
        i64 DesiredTabletSize;
    };

    const TTabletBalancerConfigPtr Config_;
    const NCellMaster::TBootstrap* Bootstrap_;
    const NConcurrency::TPeriodicExecutorPtr BalanceExecutor_;
    const NConcurrency::TPeriodicExecutorPtr EnabledCheckExecutor_;

    bool Enabled_ = false;
    bool Started_ = false;
    std::deque<TTabletId> TabletIdQueue_;
    yhash_set<TTabletId> QueuedTabletIds_;
    yhash_set<const TTablet*> TouchedTablets_;

    const NProfiling::TProfiler Profiler;
    NProfiling::TSimpleCounter MemoryMoveCounter_;
    NProfiling::TSimpleCounter MergeCounter_;
    NProfiling::TSimpleCounter SplitCounter_;
    NProfiling::TSimpleCounter QueueSizeCounter_;

    int MergeCount_ = 0;
    int SplitCount_ = 0;

    bool BalanceCells_ = false;

    void Balance()
    {
        if (!Enabled_) {
            return;
        }

        if (CheckActiveTabletActions()) {
            return;
        }

        if (BalanceCells_) {
            BalanceTabletCells();
        } else {
            BalanceTablets();
        }

        BalanceCells_ = !BalanceCells_;
    }

    bool CheckActiveTabletActions()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletActions()) {
            const auto* action = pair.second;

            if (action->GetState() != ETabletActionState::Completed &&
                action->GetState() != ETabletActionState::Failed)
            {
                return true;
            }
        }

        return false;
    }

    void BalanceTabletCells()
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        const auto& cells = tabletManager->TabletCells();

        if (cells.size() < 2) {
            return;
        }

        PROFILE_TIMING("/balance_cells_memory") {
            ReassignInMemoryTablets();
        }

        // TODO(savrus) balance other tablets.
    }

    void ReassignInMemoryTablets(const TTabletCellBundle* bundle, int* actionCount)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        std::vector<const TTabletCell*> cells;

        for (const auto& pair : tabletManager->TabletCells()) {
            if (pair.second->GetCellBundle() == bundle) {
                cells.push_back(pair.second);
            }
        }

        if (cells.empty()) {
            return;
        }

        using TMemoryUsage = std::pair<i64, const TTabletCell*>;
        std::vector<TMemoryUsage> memoryUsage;
        i64 total = 0;
        memoryUsage.reserve(cells.size());
        for (const auto* cell : cells) {
            i64 size = cell->TotalStatistics().MemorySize;
            total += size;
            memoryUsage.emplace_back(size, cell);
        }

        std::sort(memoryUsage.begin(), memoryUsage.end());
        i64 mean = total / cells.size();
        std::priority_queue<TMemoryUsage, std::vector<TMemoryUsage>, std::greater<TMemoryUsage>> queue;

        for (const auto& pair : memoryUsage) {
            if (pair.first >= mean) {
                break;
            }
            queue.push(pair);
        }

        for (int index = memoryUsage.size() - 1; index >= 0; --index) {
            auto cellSize = memoryUsage[index].first;
            auto* cell = memoryUsage[index].second;

            for (const auto* tablet : cell->Tablets()) {
                if (tablet->GetInMemoryMode() == EInMemoryMode::None) {
                    continue;
                }

                if (queue.empty() || cellSize <= mean) {
                    break;
                }

                auto top = queue.top();

                if (static_cast<double>(cellSize - top.first) / cellSize < Config_->CellBalanceFactor) {
                    break;
                }

                auto statistics = tabletManager->GetTabletStatistics(tablet);
                auto tabletSize = statistics.MemorySize;

                if (tabletSize == 0) {
                    continue;
                }

                if (tabletSize < cellSize - top.first) {
                    LOG_DEBUG("Tablet balancer would like to move tablet (TabletId: %v, SrcCellId: %v, DstCellId: %v)",
                        tablet->GetId(),
                        cell->GetId(),
                        top.second->GetId());

                    queue.pop();
                    top.first += tabletSize;
                    cellSize -= tabletSize;
                    if (top.first < mean) {
                        queue.push(top);
                    }

                    TReqCreateTabletAction request;
                    request.set_kind(static_cast<int>(ETabletActionKind::Move));
                    ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});
                    ToProto(request.mutable_cell_ids(), std::vector<TTabletCellId>{top.second->GetId()});

                    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
                    CreateMutation(hydraManager, request)
                        ->CommitAndLog(Logger);

                    ++(*actionCount);
                }
            }
        }
    }

    void ReassignInMemoryTablets()
    {
        if (!Config_->EnableInMemoryBalancer) {
            return;
        }

        int actionCount = 0;
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        for (const auto& pair : tabletManager->TabletCellBundles()) {
            ReassignInMemoryTablets(pair.second, &actionCount);
        }

        Profiler.Update(MemoryMoveCounter_, actionCount);
    }

    void BalanceTablets()
    {
        PROFILE_TIMING("/balance_tablets") {
            DoBalanceTablets();
        }

        TouchedTablets_.clear();

        Profiler.Update(QueueSizeCounter_, TabletIdQueue_.size());
        Profiler.Update(MergeCounter_, MergeCount_);
        Profiler.Update(SplitCounter_, SplitCount_);
        MergeCount_ = 0;
        SplitCount_ = 0;
    }

    void DoBalanceTablets()
    {
        // TODO(savrus) limit duration of single execution.
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        while (!TabletIdQueue_.empty()) {
            auto tabletId = TabletIdQueue_.front();
            TabletIdQueue_.pop_front();
            QueuedTabletIds_.erase(tabletId);

            auto* tablet = tabletManager->FindTablet(tabletId);
            if (!tablet ||
                !IsObjectAlive(tablet) ||
                !IsObjectAlive(tablet->GetTable()) ||
                !tablet->Replicas().empty() ||
                !IsTabletUntouched(tablet))
            {
                continue;
            }

            auto size = GetTabletSize(tablet);
            auto bounds = GetTabletSizeConfig(tablet);
            if (size < bounds.MinTabletSize || size > bounds.MaxTabletSize) {
                MergeSplitTablet(tablet, bounds);
            }
        }
    }

    i64 GetTabletSize(TTablet* tablet)
    {
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto statistics = tabletManager->GetTabletStatistics(tablet);
        return tablet->GetInMemoryMode() == EInMemoryMode::None
            ? statistics.UncompressedDataSize
            : statistics.MemorySize;
    }

    bool IsTabletUntouched(TTablet* tablet)
    {
        return TouchedTablets_.find(tablet) == TouchedTablets_.end();
    }

    void MergeSplitTablet(TTablet* tablet, const TTabletSizeConfig& bounds)
    {
        auto* table = tablet->GetTable();

        i64 desiredSize = bounds.DesiredTabletSize;
        i64 size = GetTabletSize(tablet);

        int startIndex = tablet->GetIndex();
        int endIndex = tablet->GetIndex();

        auto sizeGood = [&] () {
            i64 tabletCount = size / desiredSize;
            if (tabletCount == 0) {
                return false;
            }

            i64 tabletSize = size / tabletCount;
            return tabletSize >= bounds.MinTabletSize && tabletSize <= bounds.MaxTabletSize;
        };

        while (!sizeGood() &&
            startIndex > 0 &&
            IsTabletUntouched(table->Tablets()[startIndex - 1]) &&
            table->Tablets()[startIndex - 1]->GetState() == tablet->GetState())
        {
            --startIndex;
            size += GetTabletSize(table->Tablets()[startIndex]);
        }
        while (!sizeGood() &&
            endIndex < table->Tablets().size() - 1 &&
            IsTabletUntouched(table->Tablets()[endIndex + 1]) &&
            table->Tablets()[endIndex + 1]->GetState() == tablet->GetState())
        {
            ++endIndex;
            size += GetTabletSize(table->Tablets()[endIndex]);
        }

        int newTabletCount = size / desiredSize;
        if (newTabletCount == 0) {
            newTabletCount = 1;
        }

        if (newTabletCount == endIndex - startIndex + 1) {
            return;
        }

        std::vector<TTabletId> tabletIds;
        for (int index = startIndex; index <= endIndex; ++index) {
            tabletIds.push_back(table->Tablets()[index]->GetId());
            TouchedTablets_.insert(table->Tablets()[index]);
        }

        LOG_DEBUG("Tablet balancer would like to reshard tablets (TabletIds: %v, NewTabletCount: %v)",
            tabletIds,
            newTabletCount);

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Reshard));
        ToProto(request.mutable_tablet_ids(), tabletIds);
        request.set_tablet_count(newTabletCount);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
        ++MergeCount_;
    }

    void SplitTablet(TTablet* tablet, std::pair<i64, i64> bounds)
    {
        i64 desiredSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? Config_->DesiredTabletSize
            : Config_->DesiredInMemoryTabletSize;

        int newTabletCount = DivCeil(GetTabletSize(tablet), desiredSize);

        if (newTabletCount < 2) {
            return;
        }

        LOG_DEBUG("Tablet balancer would like to reshard tablet (TabletId: %v, NewTabletCount: %v)",
            tablet->GetId(),
            newTabletCount);

        TouchedTablets_.insert(tablet);

        TReqCreateTabletAction request;
        request.set_kind(static_cast<int>(ETabletActionKind::Reshard));
        ToProto(request.mutable_tablet_ids(), std::vector<TTabletId>{tablet->GetId()});
        request.set_tablet_count(newTabletCount);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
        ++SplitCount_;
    }

    TTabletSizeConfig GetTabletSizeConfig(TTablet* tablet)
    {
        i64 minTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? Config_->MinTabletSize
            : Config_->MinInMemoryTabletSize;
        i64 maxTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? Config_->MaxTabletSize
            : Config_->MaxInMemoryTabletSize;
        i64 desiredTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? Config_->DesiredTabletSize
            : Config_->DesiredInMemoryTabletSize;

        auto* table = tablet->GetTable();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        const auto& tableAttributes = tableProxy->Attributes();

        if (tableAttributes.Get<bool>("disable_tablet_balancer", false)) {
            return TTabletSizeConfig{0, std::numeric_limits<i64>::max(), 0};
        }

        auto tableMinTabletSize = tableAttributes.Find<i64>("min_tablet_size");
        auto tableMaxTabletSize = tableAttributes.Find<i64>("max_tablet_size");
        auto tableDesiredTabletSize = tableAttributes.Find<i64>("desired_tablet_size");

        if (tableMinTabletSize && tableMaxTabletSize && tableDesiredTabletSize &&
            *tableMinTabletSize < *tableDesiredTabletSize &&
            *tableDesiredTabletSize < *tableMaxTabletSize)
        {
            minTabletSize = *tableMinTabletSize;
            maxTabletSize = *tableMaxTabletSize;
            desiredTabletSize = *tableDesiredTabletSize;
        }

        return TTabletSizeConfig{minTabletSize, maxTabletSize, desiredTabletSize};
    }


    void OnCheckEnabled()
    {
        if (!Bootstrap_->IsPrimaryMaster()) {
            return;
        }

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        if (!hydraFacade->GetHydraManager()->IsActiveLeader()) {
            return;
        }

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            return;
        }

        auto wasEnabled = Enabled_;

        try {
            Enabled_ = IsEnabled();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating tablet balancer state, disabling until the next attempt");
            Enabled_ = false;
        }

        if (Enabled_ && !wasEnabled) {
            LOG_INFO("Tablet balancer enabled");
        }
    }

    bool IsEnabled()
    {
        return
            IsEnabledByFlag() &&
            IsEnabledWorkHours();
    }

    bool IsEnabledByFlag()
    {
        bool enabled = true;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto sysNode = resolver->ResolvePath("//sys");
        if (sysNode->Attributes().Get<bool>("disable_tablet_balancer", false)) {
            if (Enabled_) {
                LOG_INFO("Tablet balancer is disabled by //sys/@disable_tablet_balancer setting");
            }
            enabled = false;
        }
        return enabled;
    }

    bool IsEnabledWorkHours()
    {
        bool enabled = true;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto sysNode = resolver->ResolvePath("//sys");
        auto officeHours = sysNode->Attributes().Find<std::vector<int>>("tablet_balancer_office_hours");
        if (!officeHours) {
            return enabled;
        }
        if (officeHours->size() != 2) {
            LOG_INFO("Expected two integers in //sys/@tablet_balancer_office_hours, but got %v",
                *officeHours);
            return enabled;
        }

        tm localTime;
        Now().LocalTime(&localTime);
        int hour = localTime.tm_hour;
        if (hour < (*officeHours)[0] || hour > (*officeHours)[1]) {
            if (Enabled_) {
                LOG_INFO("Tablet balancer is disabled by //sys/@tablet_balancer_office_hours");
            }
            enabled = false;
        }
        return enabled;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletBalancer::TTabletBalancer(
    TTabletBalancerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TTabletBalancer::~TTabletBalancer() = default;

void TTabletBalancer::Start()
{
    Impl_->Start();
}

void TTabletBalancer::Stop()
{
    Impl_->Stop();
}

void TTabletBalancer::OnTabletHeartbeat(TTablet* tablet)
{
    Impl_->OnTabletHeartbeat(tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

