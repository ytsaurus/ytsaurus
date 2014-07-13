#include "stdafx.h"
#include "tablet_cell_proxy.h"
#include "tablet_cell.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "private.h"

#include <core/ytree/fluent.h>

#include <ytlib/tablet_client/config.h>

#include <server/object_server/object_detail.h>

#include <server/node_tracker_server/node.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletCellProxy
    : public TNonversionedObjectProxyBase<TTabletCell>
{
public:
    TTabletCellProxy(NCellMaster::TBootstrap* bootstrap, TTabletCell* cell)
        : TBase(bootstrap, cell)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTabletCell> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return TabletServerLogger;
    }

    virtual void ValidateRemoval() override
    {
        const auto* cell = GetThisTypedImpl();
        if (!cell->Tablets().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove a cell with active tablets");
        }
    }

    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override
    {
        // Prevent changing options after creation.
        static auto optionsKeys = New<TTabletCellOptions>()->GetRegisteredKeys();
        if (std::find(optionsKeys.begin(), optionsKeys.end(), key) != optionsKeys.end()) {
            THROW_ERROR_EXCEPTION("Cannot change tablet cell options after creation");
        }

        return TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("size");
        attributes->push_back("state");
        attributes->push_back("health");
        attributes->push_back("peers");
        attributes->push_back("tablet_ids");
        attributes->push_back("config_version");

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        auto* cell = GetThisTypedImpl();

        if (key == "size") {
            BuildYsonFluently(consumer)
                .Value(cell->GetSize());
            return true;
        }

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(cell->GetState());
            return true;
        }

        if (key == "health") {
            BuildYsonFluently(consumer)
                .Value(cell->GetHealth());
            return true;
        }

        if (key == "peers") {
            BuildYsonFluently(consumer)
                .DoListFor(cell->Peers(), [&] (TFluentList fluent, const TTabletCell::TPeer& peer) {
                    if (peer.Address) {
                        const auto* slot = peer.Node ? &peer.Node->TabletSlots()[peer.SlotIndex] : nullptr;
                        auto state = slot ? slot->PeerState : EPeerState(EPeerState::None);
                        fluent
                            .Item().BeginMap()
                                .Item("address").Value(*peer.Address)
                                .Item("state").Value(state)
                                .Item("last_seen_time").Value(peer.LastSeenTime)
                                .DoIf(peer.Node, [&] (TFluentMap fluent) {
                                    fluent
                                        .Item("slot_index").Value(peer.SlotIndex);
                                })
                            .EndMap();
                    } else {
                        fluent
                            .Item().BeginMap()
                                .Item("state").Value(EPeerState(EPeerState::None))
                            .EndMap();
                    }
                });
            return true;
        }

        if (key == "tablet_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(cell->Tablets(), [] (TFluentList fluent, const TTablet* tablet) {
                    fluent
                        .Item().Value(tablet->GetId());
                });
            return true;
        }

        if (key == "config_version") {
            BuildYsonFluently(consumer)
                .Value(cell->GetConfigVersion());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

IObjectProxyPtr CreateTabletCellProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTabletCell* cell)
{
    return New<TTabletCellProxy>(bootstrap, cell);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

