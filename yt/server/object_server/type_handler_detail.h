#pragma once

#include "object_detail.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/transaction_server/public.h>

#include <yt/core/ytree/ypath_detail.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectTypeMetadata
{
    NYTree::TBuiltinAttributeKeysCache BuiltinAttributeKeysCache;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    explicit TObjectTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        YCHECK(bootstrap);
    }

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

    virtual TCellTagList GetReplicationCellTags(const TObjectBase* object) override
    {
        return DoGetReplicationCellTags(static_cast<const TObject*>(object));
    }

    virtual Stroka GetName(const TObjectBase* object) override
    {
        return DoGetName(static_cast<const TObject*>(object));
    }

    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(static_cast<TObject*>(object), transaction);
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& /*hintId*/,
        NYTree::IAttributeDictionary* /*attributes*/) override
    {
        YUNREACHABLE();
    }

    virtual void ZombifyObject(TObjectBase* object) throw() override
    {
        DoZombifyObject(static_cast<TObject*>(object));
    }

    virtual void UnstageObject(TObjectBase* object, bool recursive) override
    {
        DoUnstageObject(static_cast<TObject*>(object), recursive);
    }

    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObjectBase* object) override
    {
        return DoFindAcd(static_cast<TObject*>(object));
    }

    virtual TObjectBase* GetParent(TObjectBase* object) override
    {
        return DoGetParent(static_cast<TObject*>(object));
    }

    virtual NYTree::EPermissionSet GetSupportedPermissions() const override
    {
        return
            NYTree::EPermissionSet::Read |
            NYTree::EPermissionSet::Write |
            NYTree::EPermissionSet::Remove |
            NYTree::EPermissionSet::Administer;
    }

    virtual void ExportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag) override
    {
        DoExportObject(static_cast<TObject*>(object), destinationCellTag);
    }

    virtual void UnexportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag,
        int importRefCounter) override
    {
        DoUnexportObject(static_cast<TObject*>(object), destinationCellTag, importRefCounter);
    }

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    TObjectTypeMetadata Metadata_;


    virtual TCellTagList DoGetReplicationCellTags(const TObject* /*object*/)
    {
        return EmptyCellTags();
    }

    virtual Stroka DoGetName(const TObject* object) = 0;

    virtual IObjectProxyPtr DoGetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual void DoZombifyObject(TObject* /*object*/)
    { }

    virtual void DoUnstageObject(TObject* /*object*/, bool /*recursive*/)
    { }

    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TObject* /*object*/)
    {
        return nullptr;
    }

    virtual TObjectBase* DoGetParent(TObject* /*object*/)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->FindSchema(GetType());
    }

    virtual void DoExportObject(
        TObject* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/)
    {
        YUNREACHABLE();
    }

    virtual void DoUnexportObject(
        TObject* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/,
        int /*importRefCounter*/)
    {
        YUNREACHABLE();
    }


    TCellTagList EmptyCellTags()
    {
        return TCellTagList();
    }

    TCellTagList AllSecondaryCellTags()
    {
        return Bootstrap_->GetMulticellManager()->GetRegisteredMasterCellTags();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerWithMapBase
    : public TObjectTypeHandlerBase<TObject>
{
public:
    typedef typename NHydra::TEntityMap<TObject> TMap;

    TObjectTypeHandlerWithMapBase(NCellMaster::TBootstrap* bootstrap, TMap* map)
        : TObjectTypeHandlerBase<TObject>(bootstrap)
        , Map_(map)
    { }

    virtual void DestroyObject(TObjectBase* object) throw() override
    {
        this->DoDestroyObject(static_cast<TObject*>(object));
        // Remove the object from the map but keep it alive.
        Map_->Release(object->GetId()).release();
    }

    virtual NObjectServer::TObjectBase* FindObject(const TObjectId& id) override
    {
        return Map_->Find(id);
    }

    virtual void ResetAllObjects() override
    {
        for (const auto& pair : *Map_) {
            auto* object = pair.second;
            this->DoResetObject(object);
        }
    }

protected:
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* const Map_;


    virtual void DoDestroyObject(TObject* object)
    {
        // Clear ACD, if any.
        auto* acd = this->FindAcd(object);
        if (acd) {
            acd->Clear();
        }
    }

    virtual void DoResetObject(TObject* object)
    {
        object->ResetWeakRefCounter();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

