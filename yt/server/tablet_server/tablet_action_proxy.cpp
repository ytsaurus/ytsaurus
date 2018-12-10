#include "tablet_action_proxy.h"
#include "tablet_action.h"
#include "tablet_manager.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/tablet_client/config.h>

namespace NYT::NTabletServer {

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

        attributes->push_back(EInternedAttributeKey::Kind);
        attributes->push_back(EInternedAttributeKey::State);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::KeepFinished)
            .SetWritable(true));
        attributes->push_back(EInternedAttributeKey::SkipFreezing);
        attributes->push_back(EInternedAttributeKey::Freeze);
        attributes->push_back(EInternedAttributeKey::TabletIds);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::CellIds)
            .SetPresent(!action->TabletCells().empty()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKeys)
            .SetPresent(!action->PivotKeys().empty()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCount)
            .SetPresent(!!action->GetTabletCount()));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Error)
            .SetPresent(!action->Error().IsOK()));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* action = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Kind:
                BuildYsonFluently(consumer)
                    .Value(action->GetKind());
                return true;

            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(action->GetState());
                return true;

            case EInternedAttributeKey::KeepFinished:
                BuildYsonFluently(consumer)
                    .Value(action->GetKeepFinished());
                return true;

            case EInternedAttributeKey::SkipFreezing:
                BuildYsonFluently(consumer)
                    .Value(action->GetSkipFreezing());
                return true;

            case EInternedAttributeKey::Freeze:
                BuildYsonFluently(consumer)
                    .Value(action->GetFreeze());
                return true;

            case EInternedAttributeKey::TabletIds:
                BuildYsonFluently(consumer)
                    .DoListFor(action->Tablets(), [] (TFluentList fluent, const TTablet* tablet) {
                        fluent
                            .Item().Value(tablet->GetId());
                    });
                return true;

            case EInternedAttributeKey::CellIds:
                if (action->TabletCells().empty()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .DoListFor(action->TabletCells(), [] (TFluentList fluent, const TTabletCell* cell) {
                        fluent
                            .Item().Value(cell->GetId());
                    });
                return true;

            case EInternedAttributeKey::PivotKeys:
                if (action->PivotKeys().empty()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .DoListFor(action->PivotKeys(), [] (TFluentList fluent, TOwningKey key) {
                        fluent
                            .Item().Value(key);
                    });
                return true;

            case EInternedAttributeKey::TabletCount:
                if (!action->GetTabletCount()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(*action->GetTabletCount());
                return true;

            case EInternedAttributeKey::Error:
                if (action->Error().IsOK()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(action->Error());
                return true;
            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* action = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::KeepFinished:
                if (action->GetState() == ETabletActionState::Completed ||
                    action->GetState() == ETabletActionState::Failed)
                {
                    THROW_ERROR_EXCEPTION("Tablet action is already in %Qlv state",
                        action->GetState());
                }

                action->SetKeepFinished(ConvertTo<bool>(value));
                return true;
            default:
                break;
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

} // namespace NYT::NTabletServer

