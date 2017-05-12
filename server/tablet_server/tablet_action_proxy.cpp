#include "tablet_action_proxy.h"
#include "tablet_action.h"
#include "tablet_manager.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletActionProxy
    : public TNonversionedObjectProxyBase<TTabletAction>
{
public:
    TTabletActionProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTabletAction* action)
        : TBase(bootstrap, metadata, action)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTabletAction> TBase;

    virtual void ValidateRemoval() override
    { }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        const auto* action = GetThisImpl();

        attributes->push_back("kind");
        attributes->push_back("state");
        attributes->push_back("keep_finished");
        attributes->push_back("skip_freezing");
        attributes->push_back("freeze");
        attributes->push_back("tablet_ids");
        attributes->push_back(TAttributeDescriptor("cell_ids")
            .SetPresent(!action->TabletCells().empty()));
        attributes->push_back(TAttributeDescriptor("pivot_keys")
            .SetPresent(!action->PivotKeys().empty()));
        attributes->push_back(TAttributeDescriptor("tablet_count")
            .SetPresent(!!action->GetTabletCount()));
        attributes->push_back(TAttributeDescriptor("error")
            .SetPresent(!action->Error().IsOK()));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto* action = GetThisImpl();

        if (key == "kind") {
            BuildYsonFluently(consumer)
                .Value(action->GetKind());
            return true;
        }

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(action->GetState());
            return true;
        }

        if (key == "keep_finished") {
            BuildYsonFluently(consumer)
                .Value(action->GetKeepFinished());
            return true;
        }

        if (key == "skip_freezing") {
            BuildYsonFluently(consumer)
                .Value(action->GetSkipFreezing());
            return true;
        }

        if (key == "freeze") {
            BuildYsonFluently(consumer)
                .Value(action->GetFreeze());
            return true;
        }

        if (key == "tablet_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(action->Tablets(), [] (TFluentList fluent, const TTablet* tablet) {
                    fluent
                        .Item().Value(tablet->GetId());
                });
            return true;
        }

        if (key == "cell_ids" && !action->TabletCells().empty()) {
            BuildYsonFluently(consumer)
                .DoListFor(action->TabletCells(), [] (TFluentList fluent, const TTabletCell* cell) {
                    fluent
                        .Item().Value(cell->GetId());
                });
            return true;
        }

        if (key == "pivot_keys" && !action->PivotKeys().empty()) {
            BuildYsonFluently(consumer)
                .DoListFor(action->PivotKeys(), [] (TFluentList fluent, TOwningKey key) {
                    fluent
                        .Item().Value(key);
                });
            return true;
        }

        if (key == "tablet_count" && action->GetTabletCount()) {
            BuildYsonFluently(consumer)
                .Value(*action->GetTabletCount());
            return true;
        }

        if (key == "error" && !action->Error().IsOK()) {
            BuildYsonFluently(consumer)
                .Value(action->Error());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        auto* action = GetThisImpl();

        if (key == "keep_finished") {
            if (action->GetState() == ETabletActionState::Completed ||
                action->GetState() == ETabletActionState::Failed)
            {
                THROW_ERROR_EXCEPTION("Tablet action is already in %Qlv state",
                    action->GetState());
            }

            action->SetKeepFinished(ConvertTo<bool>(value));
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

IObjectProxyPtr CreateTabletActionProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletAction* action)
{
    return New<TTabletActionProxy>(bootstrap, metadata, action);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

