#include "table_manager.h"
#include "private.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/table_server/proto/table_manager.pb.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/random_access_queue.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NHydra;
using namespace NSecurityServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableServerLogger;

///////////////////////////////////////////////////////////////////////////////

class TTableManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TableManager)
        , TableStatisticsGossipThrottler_(CreateReconfigurableThroughputThrottler(
            New<TThroughputThrottlerConfig>(),
            TableServerLogger,
            TableServerProfiler.WithPrefix("/table_statistics_gossip_throttler")))
    {
        RegisterLoader(
            "TableManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TableManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TableManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TableManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraSendTableStatisticsUpdates, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTableStatistics, Unretained(this)));
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        OnDynamicConfigChanged();

        const auto& gossipConfig = GetGossipConfig();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            TableStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
                BIND(&TImpl::OnTableStatisticsGossip, MakeWeak(this)),
                gossipConfig->TableStatisticsGossipPeriod);
            TableStatisticsGossipExecutor_->Start();
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        if (TableStatisticsGossipExecutor_) {
            TableStatisticsGossipExecutor_->Stop();
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TableStatisticsUpdateRequests_.Clear();
    }

    void ScheduleTableStatisticsUpdate(
        TTableNode* table,
        bool updateDataStatistics,
        bool updateTabletStatistics)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Schedule table statistics update (TableId: %v, UpdateDataStatistics: %v, UpdateTabletStatistics: %v)",
                table->GetId(),
                updateDataStatistics,
                updateTabletStatistics);

            auto& statistics = TableStatisticsUpdateRequests_[table->GetId()];
            statistics.UpdateTabletResourceUsage |= updateTabletStatistics;
            statistics.UpdateDataStatistics |= updateDataStatistics;
            statistics.UpdateModificationTime = true;
            statistics.UpdateAccessTime = true;
        }
    }

    void OnTableStatisticsGossip()
    {
        auto tableCount = TableStatisticsUpdateRequests_.Size();
        tableCount = TableStatisticsGossipThrottler_->TryAcquireAvailable(tableCount);
        if (tableCount == 0) {
            return;
        }

        NTableServer::NProto::TReqSendTableStatisticsUpdates request;
        request.set_table_count(tableCount);
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void SendTableStatisticsUpdates(TChunkOwnerBase* chunkOwner)
    {
        if (chunkOwner->IsNative()) {
            return;
        }

        YT_VERIFY(IsTableType(chunkOwner->GetType()));
        // NB: this may also be a replicated table.
        auto* table = chunkOwner->As<TTableNode>();

        SendTableStatisticsUpdates(table);
    }

    void SendTableStatisticsUpdates(TTableNode* table)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Sending table statistics update (TableId: %v)",
            table->GetId());

        NTableServer::NProto::TReqUpdateTableStatistics req;
        auto* entry = req.add_entries();
        ToProto(entry->mutable_table_id(), table->GetId());
        ToProto(entry->mutable_data_statistics(), table->SnapshotStatistics());
        ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
        entry->set_modification_time(ToProto<ui64>(table->GetModificationTime()));
        entry->set_access_time(ToProto<ui64>(table->GetAccessTime()));
        multicellManager->PostToMaster(req, table->GetNativeCellTag());

        TableStatisticsUpdateRequests_.Pop(table->GetId());
    }

    void HydraSendTableStatisticsUpdates(NTableServer::NProto::TReqSendTableStatisticsUpdates* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto remainingTableCount = request->table_count();

        std::vector<TTableId> tableIds;
        // NB: Ordered map is needed to make things deterministic.
        std::map<TCellTag, NTableServer::NProto::TReqUpdateTableStatistics> cellTagToRequest;
        while (remainingTableCount-- > 0 && !TableStatisticsUpdateRequests_.IsEmpty()) {
            const auto& [tableId, statistics] = TableStatisticsUpdateRequests_.Pop();

            auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
            if (!IsObjectAlive(node)) {
                continue;
            }
            YT_VERIFY(IsTableType(node->GetType()));
            auto* table = node->As<TTableNode>();

            auto cellTag = CellTagFromId(tableId);
            auto& request = cellTagToRequest[cellTag];
            auto* entry = request.add_entries();
            ToProto(entry->mutable_table_id(), tableId);
            if (statistics.UpdateDataStatistics) {
                ToProto(entry->mutable_data_statistics(), table->SnapshotStatistics());
            }
            if (statistics.UpdateTabletResourceUsage) {
                ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
            }
            if (statistics.UpdateModificationTime) {
                entry->set_modification_time(ToProto<ui64>(table->GetModificationTime()));
            }
            if (statistics.UpdateAccessTime) {
                entry->set_access_time(ToProto<ui64>(table->GetAccessTime()));
            }
            tableIds.push_back(tableId);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Sending table statistics update (RequestedTableCount: %v, TableIds: %v)",
            request->table_count(),
            tableIds);

        for  (const auto& [cellTag, request] : cellTagToRequest) {
            multicellManager->PostToMaster(request, cellTag);
        }
    }

    void HydraUpdateTableStatistics(NTableServer::NProto::TReqUpdateTableStatistics* request)
    {
        std::vector<TTableId> tableIds;
        tableIds.reserve(request->entries_size());
        for (const auto& entry : request->entries()) {
            tableIds.push_back(FromProto<TTableId>(entry.table_id()));
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Received table statistics update (TableIds: %v)",
            tableIds);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& entry : request->entries()) {
            auto tableId = FromProto<TTableId>(entry.table_id());
            auto* node = cypressManager->FindNode(TVersionedNodeId(tableId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            YT_VERIFY(IsTableType(node->GetType()));
            auto* table = node->As<TTableNode>();

            if (entry.has_tablet_resource_usage()) {
                auto tabletResourceUsage = FromProto<TTabletResources>(entry.tablet_resource_usage());
                table->SetExternalTabletResourceUsage(tabletResourceUsage);
            }

            if (entry.has_data_statistics()) {
                YT_VERIFY(table->IsDynamic());
                table->SnapshotStatistics() = entry.data_statistics();
            }

            if (entry.has_modification_time()) {
                table->SetModificationTime(std::max(
                    table->GetModificationTime(),
                    FromProto<TInstant>(entry.modification_time())));
            }

            if (entry.has_access_time()) {
                table->SetAccessTime(std::max(
                    table->GetAccessTime(),
                    FromProto<TInstant>(entry.access_time())));
            }
        }
    }

    void LoadTableStatisticsUpdateRequests(NCellMaster::TLoadContext& context)
    {
        Load(context, TableStatisticsUpdateRequests_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    { }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        if (context.GetVersion() >= EMasterReign::MoveTableStatisticsGossipToTableManager) {
            LoadTableStatisticsUpdateRequests(context);
        } // Otherwise loading is initiated from tablet manager.
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    { }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, TableStatisticsUpdateRequests_);
    }

private:

    const TDynamicTablesMulticellGossipConfigPtr& GetGossipConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->TabletManager->MulticellGossip;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig = nullptr)
    {
        const auto& gossipConfig = GetGossipConfig();
        TableStatisticsGossipThrottler_->Reconfigure(gossipConfig->TableStatisticsGossipThrottler);

        if (TableStatisticsGossipExecutor_) {
            TableStatisticsGossipExecutor_->SetPeriod(gossipConfig->TableStatisticsGossipPeriod);
        }
    }

    struct TTableStatisticsUpdateRequest
    {
        bool UpdateDataStatistics;
        bool UpdateTabletResourceUsage;
        bool UpdateModificationTime;
        bool UpdateAccessTime;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            // COMPAT(savrus)
            if (context.GetVersion() <= EMasterReign::TruncateJournals ||
                context.GetVersion() >= EMasterReign::TuneTabletStatisticsUpdate_20_2)
            {
                Persist(context, UpdateDataStatistics);
                Persist(context, UpdateTabletResourceUsage);
                Persist(context, UpdateModificationTime);
                Persist(context, UpdateAccessTime);
            } else {
                std::optional<TDataStatistics> dataStatistics;
                std::optional<TClusterResources> tabletResourceUsage;
                std::optional<TInstant> modificationTime;
                std::optional<TInstant> accessTime;

                Persist(context, dataStatistics);
                Persist(context, tabletResourceUsage);
                Persist(context, modificationTime);
                Persist(context, accessTime);

                UpdateDataStatistics = dataStatistics.has_value();
                UpdateTabletResourceUsage = tabletResourceUsage.has_value();
                UpdateModificationTime = modificationTime.has_value();
                UpdateAccessTime = accessTime.has_value();
            }
        }
    };

    TRandomAccessQueue<TTableId, TTableStatisticsUpdateRequest> TableStatisticsUpdateRequests_;
    TPeriodicExecutorPtr TableStatisticsGossipExecutor_;
    IReconfigurableThroughputThrottlerPtr TableStatisticsGossipThrottler_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

////////////////////////////////////////////////////////////////////////////////

TTableManager::TTableManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTableManager::~TTableManager() = default;

void TTableManager::Initialize()
{
    Impl_->Initialize();
}

void TTableManager::ScheduleTableStatisticsUpdate(
    TTableNode* table,
    bool updateDataStatistics,
    bool updateTabletStatistics)
{
    Impl_->ScheduleTableStatisticsUpdate(table, updateDataStatistics, updateTabletStatistics);
}

void TTableManager::SendTableStatisticsUpdates(TChunkOwnerBase* chunkOwner)
{
    Impl_->SendTableStatisticsUpdates(chunkOwner);
}

void TTableManager::LoadTableStatisticsUpdateRequests(NCellMaster::TLoadContext& context)
{
    Impl_->LoadTableStatisticsUpdateRequests(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
