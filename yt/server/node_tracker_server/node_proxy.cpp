#include "node.h"
#include "data_center.h"
#include "rack.h"
#include "node_tracker.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/medium.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/server/tablet_server/tablet_cell.h>

#include <yt/server/cell_master/bootstrap.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeProxy
    : public TNonversionedObjectProxyBase<TNode>
{
public:
    TClusterNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TNode* node)
        : TNonversionedObjectProxyBase(bootstrap, metadata, node)
    { }

private:
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TNonversionedObjectProxyBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor("banned")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("decommissioned")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("disable_write_sessions")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("disable_scheduler_jobs")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("disable_tablet_cells")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("rack")
            .SetPresent(node->GetRack())
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back("data_center");
        descriptors->push_back("state");
        descriptors->push_back("multicell_states");
        descriptors->push_back(TAttributeDescriptor("user_tags")
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back("tags");
        descriptors->push_back("last_seen_time");
        bool isGood = node->GetLocalState() == ENodeState::Registered || node->GetLocalState() == ENodeState::Online;
        descriptors->push_back(TAttributeDescriptor("register_time")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("lease_transaction_id")
            .SetPresent(isGood && node->GetLeaseTransaction()));
        descriptors->push_back(TAttributeDescriptor("statistics")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("full")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("addresses")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("alerts")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("alert_count")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("tablet_slots")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("io_weights")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("resource_usage")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("resource_limits")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("resource_limits_overrides")
            .SetWritable(true)
            .SetReplicated(true));
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        auto state = node->GetLocalState();
        bool isGood = state == ENodeState::Registered || state == ENodeState::Online;

        if (key == "banned") {
            BuildYsonFluently(consumer)
                .Value(node->GetBanned());
            return true;
        }

        if (key == "decommissioned") {
            BuildYsonFluently(consumer)
                .Value(node->GetDecommissioned());
            return true;
        }

        if (key == "disable_write_sessions") {
            BuildYsonFluently(consumer)
                .Value(node->GetDisableWriteSessions());
            return true;
        }

        if (key == "disable_scheduler_jobs") {
            BuildYsonFluently(consumer)
                .Value(node->GetDisableSchedulerJobs());
            return true;
        }

        if (key == "disable_tablet_cells") {
            BuildYsonFluently(consumer)
                .Value(node->GetDisableTabletCells());
            return true;
        }

        if (key == "rack" && node->GetRack()) {
            BuildYsonFluently(consumer)
                .Value(node->GetRack()->GetName());
            return true;
        }

        if (key == "data_center" &&
            node->GetRack() && node->GetRack()->GetDataCenter())
        {
            BuildYsonFluently(consumer)
                .Value(node->GetRack()->GetDataCenter()->GetName());
            return true;
        }

        if (key == "state") {
            auto state = Bootstrap_->IsPrimaryMaster()
                ? node->GetAggregatedState()
                : node->GetLocalState();
            BuildYsonFluently(consumer)
                .Value(state);
            return true;
        }

        if (key == "multicell_states") {
            BuildYsonFluently(consumer)
                .DoMapFor(node->MulticellStates(), [] (TFluentMap fluent, const std::pair<TCellTag, ENodeState>& pair) {
                    fluent.Item(ToString(pair.first)).Value(pair.second);
                });
            return true;
        }

        if (key == "user_tags") {
            BuildYsonFluently(consumer)
                .Value(node->UserTags());
            return true;
        }

        if (key == "tags") {
            BuildYsonFluently(consumer)
                .Value(node->Tags());
            return true;
        }

        if (key == "last_seen_time") {
            BuildYsonFluently(consumer)
                .Value(node->GetLastSeenTime());
            return true;
        }

        if (isGood) {
            if (key == "register_time") {
                BuildYsonFluently(consumer)
                    .Value(node->GetRegisterTime());
                return true;
            }

            if (key == "lease_transaction_id" && node->GetLeaseTransaction()) {
                BuildYsonFluently(consumer)
                    .Value(node->GetLeaseTransaction()->GetId());
                return true;
            }

            if (key == "statistics") {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& statistics = node->Statistics();
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("total_available_space").Value(statistics.total_available_space())
                        .Item("total_used_space").Value(statistics.total_used_space())
                        .Item("total_stored_chunk_count").Value(statistics.total_stored_chunk_count())
                        .Item("total_cached_chunk_count").Value(statistics.total_cached_chunk_count())
                        .Item("total_session_count").Value(node->GetTotalSessionCount())
                        .Item("full").Value(statistics.full())
                        .Item("locations").DoListFor(statistics.locations(), [&] (TFluentList fluent, const TLocationStatistics& locationStatistics) {
                            auto mediumIndex = locationStatistics.medium_index();
                            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                            if (!medium) {
                                return;
                            }
                            fluent
                                .Item().BeginMap()
                                    .Item("medium_name").Value(medium->GetName())
                                    .Item("available_space").Value(locationStatistics.available_space())
                                    .Item("used_space").Value(locationStatistics.used_space())
                                    .Item("low_watermark_space").Value(locationStatistics.low_watermark_space())
                                    .Item("chunk_count").Value(locationStatistics.chunk_count())
                                    .Item("session_count").Value(locationStatistics.session_count())
                                    .Item("full").Value(locationStatistics.full())
                                    .Item("enabled").Value(locationStatistics.enabled())
                                    .Item("throttling_reads").Value(locationStatistics.throttling_reads())
                                    .Item("throttling_writes").Value(locationStatistics.throttling_writes())
                                .EndMap();
                        })
                        .Item("media").DoMapFor(statistics.media(), [&] (TFluentMap fluent, const TMediumStatistics& mediumStatistics) {
                            auto mediumIndex = mediumStatistics.medium_index();
                            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                            if (!medium) {
                                return;
                            }
                            fluent
                                .Item(medium->GetName()).BeginMap()
                                    .Item("io_weight").Value(mediumStatistics.io_weight())
                                .EndMap();
                        })
                        .Item("memory").BeginMap()
                            .Item("total").BeginMap()
                                .Item("used").Value(statistics.memory().total_used())
                                .Item("limit").Value(statistics.memory().total_limit())
                            .EndMap()
                            .DoFor(statistics.memory().categories(), [] (TFluentMap fluent, const TMemoryStatistics::TCategory& category) {
                                fluent.Item(FormatEnum(EMemoryCategory(category.type())))
                                    .BeginMap()
                                        .DoIf(category.has_limit(), [&] (TFluentMap fluent) {
                                            fluent.Item("limit").Value(category.limit());
                                        })
                                        .Item("used").Value(category.used())
                                    .EndMap();
                            })
                        .EndMap()
                        .Item("network").BeginMap()
                            .DoFor(statistics.network(), [] (TFluentMap fluent, const TNetworkStatistics& statistics) {
                                fluent.Item(statistics.network())
                                    .BeginMap()
                                        .Item("throttling_reads").Value(statistics.throttling_reads())
                                    .EndMap();
                            })
                        .EndMap()
                    .EndMap();
                return true;
            }

            if (key == "full") {
                BuildYsonFluently(consumer)
                    .Value(node->Statistics().full());
                return true;
            }

            if (key == "alerts") {
                BuildYsonFluently(consumer)
                    .Value(node->Alerts());
                return true;
            }

            if (key == "alert_count") {
                BuildYsonFluently(consumer)
                    .Value(node->Alerts().size());
                return true;
            }

            if (key == "addresses") {
                BuildYsonFluently(consumer)
                    .Value(node->GetNodeAddresses());
                return true;
            }

            if (key == "tablet_slots") {
                BuildYsonFluently(consumer)
                    .DoListFor(node->TabletSlots(), [] (TFluentList fluent, const TNode::TTabletSlot& slot) {
                        fluent
                            .Item().BeginMap()
                            .Item("state").Value(slot.PeerState)
                            .DoIf(slot.Cell, [&] (TFluentMap fluent) {
                                fluent
                                    .Item("cell_id").Value(slot.Cell->GetId())
                                    .Item("peer_id").Value(slot.PeerId);
                            })
                            .EndMap();
                    });
                return true;
            }

            if (key == "io_weights") {
                RequireLeader();
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                BuildYsonFluently(consumer)
                    .DoMapFor(0, NChunkClient::MaxMediumCount, [&] (TFluentMap fluent, int mediumIndex) {
                        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                        if (medium) {
                            fluent
                                .Item(medium->GetName())
                                .Value(node->IOWeights()[mediumIndex]);
                        }
                    });

                return true;
            }

            if (key == "resource_usage") {
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(node->ResourceUsage());
                return true;
            }

            if (key == "resource_limits") {
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(node->ResourceLimits());
                return true;
            }
        }

        if (key == "resource_limits_overrides") {
            BuildYsonFluently(consumer)
                .Value(node->ResourceLimitsOverrides());
            return true;
        }

        return TNonversionedObjectProxyBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        auto* node = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "banned") {
            auto banned = ConvertTo<bool>(value);
            nodeTracker->SetNodeBanned(node, banned);
            return true;
        }

        if (key == "decommissioned") {
            auto decommissioned = ConvertTo<bool>(value);
            nodeTracker->SetNodeDecommissioned(node, decommissioned);
            return true;
        }

        if (key == "disable_write_sessions") {
            auto disableWriteSessions = ConvertTo<bool>(value);
            nodeTracker->SetDisableWriteSessions(node, disableWriteSessions);
            return true;
        }

        if (key == "disable_scheduler_jobs") {
            auto disableSchedulerJobs = ConvertTo<bool>(value);
            node->SetDisableSchedulerJobs(disableSchedulerJobs);
            return true;
        }

        if (key == "disable_tablet_cells") {
            auto disableTabletCells = ConvertTo<bool>(value);
            node->SetDisableTabletCells(disableTabletCells);
            return true;
        }

        if (key == "rack") {
            auto rackName = ConvertTo<TString>(value);
            auto* rack = nodeTracker->GetRackByNameOrThrow(rackName);
            nodeTracker->SetNodeRack(node, rack);
            return true;
        }

        if (key == "resource_limits_overrides") {
            node->ResourceLimitsOverrides() = ConvertTo<TNodeResourceLimitsOverrides>(value);
            return true;
        }

        if (key == "user_tags") {
            nodeTracker->SetNodeUserTags(node, ConvertTo<std::vector<TString>>(value));
            return true;
        }

        return TNonversionedObjectProxyBase::SetBuiltinAttribute(key, value);
    }

    virtual bool RemoveBuiltinAttribute(const TString& key) override
    {
        auto* node = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "rack") {
            nodeTracker->SetNodeRack(node, nullptr);
            return true;
        }

        return false;
    }

    virtual void ValidateRemoval() override
    {
        const auto* node = GetThisImpl();
        if (node->GetLocalState() != ENodeState::Offline) {
            THROW_ERROR_EXCEPTION("Cannot remove node since it is not offline");
        }
    }
};

IObjectProxyPtr CreateClusterNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TNode* node)
{
    return New<TClusterNodeProxy>(bootstrap, metadata, node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

