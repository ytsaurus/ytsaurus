#include "stdafx.h"
#include "node.h"
#include "rack.h"
#include "node_tracker.h"

#include <core/ytree/fluent.h>

#include <ytlib/node_tracker_client/public.h>

#include <server/object_server/object_detail.h>

#include <server/transaction_server/transaction.h>

#include <server/tablet_server/tablet_cell.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeProxy
    : public TNonversionedObjectProxyBase<TNode>
{
public:
    TClusterNodeProxy(TBootstrap* bootstrap, TNode* node)
        : TNonversionedObjectProxyBase(bootstrap, node)
    { }

private:
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TNonversionedObjectProxyBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisTypedImpl();

        descriptors->push_back("banned");
        descriptors->push_back("decommissioned");
        descriptors->push_back(TAttributeDescriptor("rack")
            .SetPresent(node->GetRack())
            .SetRemovable(true));
        descriptors->push_back("state");
        descriptors->push_back("multicell_states");
        bool isGood = node->GetLocalState() == ENodeState::Registered || node->GetLocalState() == ENodeState::Online;
        descriptors->push_back(TAttributeDescriptor("last_seen_time"));
        descriptors->push_back(TAttributeDescriptor("register_time")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("lease_transaction_id")
            .SetPresent(isGood && node->GetLeaseTransaction()));
        descriptors->push_back(TAttributeDescriptor("statistics")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("addresses")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("alerts")
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor("tablet_slots")
            .SetPresent(isGood));
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisTypedImpl();
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

        if (key == "rack" && node->GetRack()) {
            BuildYsonFluently(consumer)
                .Value(node->GetRack()->GetName());
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
                const auto& statistics = node->Statistics();
                BuildYsonFluently(consumer)
                    .BeginMap()
                    .Item("total_available_space").Value(statistics.total_available_space())
                    .Item("total_used_space").Value(statistics.total_used_space())
                    .Item("total_stored_chunk_count").Value(statistics.total_stored_chunk_count())
                    .Item("total_cached_chunk_count").Value(statistics.total_cached_chunk_count())
                    .Item("total_session_count").Value(node->GetTotalSessionCount())
                    .Item("full").Value(statistics.full())
                    .Item("accepted_chunk_types").Value(FromProto<EObjectType, std::vector<EObjectType>>(statistics.accepted_chunk_types()))
                    .Item("locations").DoListFor(statistics.locations(), [] (TFluentList fluent, const TLocationStatistics& locationStatistics) {
                        fluent
                            .Item().BeginMap()
                            .Item("available_space").Value(locationStatistics.available_space())
                            .Item("used_space").Value(locationStatistics.used_space())
                            .Item("chunk_count").Value(locationStatistics.chunk_count())
                            .Item("session_count").Value(locationStatistics.session_count())
                            .Item("full").Value(locationStatistics.full())
                            .Item("enabled").Value(locationStatistics.enabled())
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
                    .EndMap();
                return true;
            }

            if (key == "alerts") {
                BuildYsonFluently(consumer)
                    .Value(node->Alerts());
                return true;
            }

            if (key == "addresses") {
                BuildYsonFluently(consumer)
                    .Value(node->GetDescriptor().Addresses());
                return true;
            }

            // XXX(babenko): multicell?
            if (key == "stored_replica_count") {
                BuildYsonFluently(consumer)
                    .Value(node->StoredReplicas().size());
                return true;
            }

            // XXX(babenko): multicell?
            if (key == "cached_replica_count") {
                BuildYsonFluently(consumer)
                    .Value(node->CachedReplicas().size());
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
        }

        return TNonversionedObjectProxyBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        auto* node = GetThisTypedImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

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

        if (key == "rack") {
            auto rackName = ConvertTo<Stroka>(value);
            auto* rack = nodeTracker->GetRackByNameOrThrow(rackName);
            nodeTracker->SetNodeRack(node, rack);
            return true;
        }

        return TNonversionedObjectProxyBase::SetBuiltinAttribute(key, value);
    }

    virtual bool RemoveBuiltinAttribute(const Stroka& key) override
    {
        auto* node = GetThisTypedImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "rack") {
            nodeTracker->SetNodeRack(node, nullptr);
            return true;
        }

        return false;
    }

    virtual void ValidateRemoval() override
    {
        const auto* node = GetThisTypedImpl();
        if (node->GetLocalState() != ENodeState::Offline) {
            THROW_ERROR_EXCEPTION("Cannot remove node since it is not offline");
        }
    }

};

IObjectProxyPtr CreateClusterNodeProxy(TBootstrap* bootstrap, TNode* node)
{
    YASSERT(bootstrap);
    YASSERT(node);

    return New<TClusterNodeProxy>(bootstrap, node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

