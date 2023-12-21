#include "tablet_hydra_service.h"

#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/table_server/table_manager.h>
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTabletNode;
using namespace NTabletClient;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TTabletHydraService
    : public TMasterHydraServiceBase
{
public:
    explicit TTabletHydraService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TMasterTabletServiceProxy::GetDescriptor(),
            NCellMaster::EAutomatonThreadQueue::TabletService,
            TabletServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetTableBalancingAttributes)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, GetTableBalancingAttributes)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);

        bool balancingAttributesRequested = request->fetch_balancing_attributes();
        bool statisticsRequested = request->fetch_statistics();

        context->SetRequestInfo("BalancingAttributesRequested: %v, StatisticsRequested: %v",
            balancingAttributesRequested,
            statisticsRequested);

        SyncWithUpstream();

        auto requestedPerformanceCounters = FromProto<std::vector<TString>>(
            request->requested_performance_counters());

        static const std::vector<TString> statisticsFieldNames{
            // i64 fields
            "unmerged_row_count",
            "uncompressed_data_size",
            "compressed_data_size",
            "hunk_uncompressed_data_size",
            "hunk_compressed_data_size",
            "memory_size",
            "dynamic_memory_pool_size",
            "disk_space",

            // i32 fields
            "chunk_count",
            "partition_count",
            "store_count",
            "preload_pending_store_count",
            "preload_completed_store_count",
            "preload_failed_store_count",
            "tablet_count",
            "overlapping_store_count"
        };

        static const THashMap<TString, TEmaCounter TTabletPerformanceCounters::*> performanceCounterFields = {
            #define XX(name, Name) \
                {#name, &TTabletPerformanceCounters::Name},
                ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
            #undef XX
        };

        std::vector<TEmaCounter TTabletPerformanceCounters::*> requestedFields;
        for (const auto& counter : requestedPerformanceCounters) {
            if (auto it = performanceCounterFields.find(counter); it != performanceCounterFields.end()) {
                requestedFields.push_back(it->second);
            } else {
                THROW_ERROR_EXCEPTION("Requested performance counter was not found: %v",
                    counter);
            }
        }

        auto fillStatistics = [] (
            const TTabletStatistics& statistics,
            auto* protoStatistics,
            const std::vector<TString>& statisticsFieldNames)
        {
            i64 diskSpace = 0;
            for (const auto& [mediumIndex, mediumDiskSpace] : statistics.DiskSpacePerMedium) {
                diskSpace += mediumDiskSpace;
            }

            protoStatistics->add_i64_fields(statistics.UnmergedRowCount);
            protoStatistics->add_i64_fields(statistics.UncompressedDataSize);
            protoStatistics->add_i64_fields(statistics.CompressedDataSize);
            protoStatistics->add_i64_fields(statistics.HunkUncompressedDataSize);
            protoStatistics->add_i64_fields(statistics.HunkCompressedDataSize);
            protoStatistics->add_i64_fields(statistics.MemorySize);
            protoStatistics->add_i64_fields(statistics.DynamicMemoryPoolSize);
            protoStatistics->add_i64_fields(diskSpace);

            protoStatistics->add_i32_fields(statistics.ChunkCount);
            protoStatistics->add_i32_fields(statistics.PartitionCount);
            protoStatistics->add_i32_fields(statistics.StoreCount);
            protoStatistics->add_i32_fields(statistics.PreloadPendingStoreCount);
            protoStatistics->add_i32_fields(statistics.PreloadCompletedStoreCount);
            protoStatistics->add_i32_fields(statistics.PreloadFailedStoreCount);
            protoStatistics->add_i32_fields(statistics.TabletCount);
            protoStatistics->add_i32_fields(statistics.OverlappingStoreCount);

            YT_VERIFY(std::ssize(statisticsFieldNames) ==
                protoStatistics->i64_fields_size() + protoStatistics->i32_fields_size());
        };

        auto fillPerformanceCounters = [] (
            const std::vector<TEmaCounter TTabletPerformanceCounters::*>& requestedFields,
            const TTabletPerformanceCounters& performanceCounters,
            auto* protoPerformanceCounters)
        {
            for (const auto& field : requestedFields) {
                auto* protoCounter = protoPerformanceCounters->Add();
                protoCounter->set_count((performanceCounters.*field).Count);
                protoCounter->set_rate((performanceCounters.*field).ImmediateRate);
                protoCounter->set_rate_10m((performanceCounters.*field).WindowRates[0]);
                protoCounter->set_rate_1h((performanceCounters.*field).WindowRates[1]);
            }
        };

        if (statisticsRequested) {
            ToProto(response->mutable_statistics_field_names(), statisticsFieldNames);
        }

        const auto& tableManager = Bootstrap_->GetTableManager();
        auto tableIds = FromProto<std::vector<NTableClient::TTableId>>(request->table_ids());
        for (auto tableId : tableIds) {
            auto* protoTable = response->add_tables();

            THROW_ERROR_EXCEPTION_IF(!IsTableType(TypeFromId(tableId)),
                "Unexpected table type %v",
                TypeFromId(tableId));

            auto* table = tableManager->FindTableNode(tableId);
            if (!table) {
                // The table is not available or has already been deleted.
                continue;
            }

            if (balancingAttributesRequested) {
                auto* balancingAttributes = protoTable->mutable_balancing_attributes();

                balancingAttributes->set_dynamic(table->IsDynamic());
                balancingAttributes->set_in_memory_mode(ToProto<int>(table->GetInMemoryMode()));

                auto config = ConvertToYsonString(table->TabletBalancerConfig()).ToString();
                balancingAttributes->set_tablet_balancer_config(config);
            }

            if (statisticsRequested) {
                for (const auto& tabletBase : table->Tablets()) {
                    YT_VERIFY(tabletBase->GetType() == EObjectType::Tablet);

                    auto* tablet = tabletBase->As<TTablet>();
                    auto* protoTablet = protoTable->add_tablets();
                    ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());

                    if (auto* cell = tablet->GetCell()) {
                        ToProto(protoTablet->mutable_cell_id(), cell->GetId());
                        protoTablet->set_mount_time(ToProto<i64>(tablet->Servant().GetMountTime()));
                    }

                    protoTablet->set_state(ToProto<int>(tablet->GetState()));
                    protoTablet->set_index(tablet->GetIndex());

                    auto statistics = tablet->GetTabletStatistics();
                    auto* protoStatistics = protoTablet->mutable_statistics();
                    fillStatistics(statistics, protoStatistics, statisticsFieldNames);

                    auto performanceCounters = tablet->PerformanceCounters();
                    auto* protoPerformanceCounters = protoTablet->mutable_performance_counters();
                    fillPerformanceCounters(requestedFields, performanceCounters, protoPerformanceCounters);
                }
            }

            for (const auto& userAttributeKey : request->user_attribute_keys()) {
                auto attribute = table->FindAttribute(userAttributeKey);
                if (attribute) {
                    protoTable->add_user_attributes(attribute->ToString());
                } else {
                    protoTable->add_user_attributes(TString{});
                }
            }
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTabletHydraService(TBootstrap* bootstrap)
{
    return New<TTabletHydraService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
