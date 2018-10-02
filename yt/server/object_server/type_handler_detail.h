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
        return DoGetReplicationCellTags(object->As<TObject>());
    }

    virtual TString GetName(const TObjectBase* object) override
    {
        return DoGetName(object->As<TObject>());
    }

    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(object->As<TObject>(), transaction);
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& /*hintId*/,
        NYTree::IAttributeDictionary* /*attributes*/) override
    {
        Y_UNREACHABLE();
    }

    virtual std::unique_ptr<TObjectBase> InstantiateObject(const TObjectId& /*id*/) override
    {
        Y_UNREACHABLE();
    }

    virtual void ZombifyObject(TObjectBase* object) noexcept override
    {
        DoZombifyObject(object->As<TObject>());
    }

    virtual void UnstageObject(TObjectBase* object, bool recursive) override
    {
        DoUnstageObject(object->As<TObject>(), recursive);
    }

    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObjectBase* object) override
    {
        return DoFindAcd(object->As<TObject>());
    }

    virtual TObjectBase* GetParent(TObjectBase* object) override
    {
        return DoGetParent(object->As<TObject>());
    }

    virtual void ExportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag) override
    {
        DoExportObject(object->As<TObject>(), destinationCellTag);
    }

    virtual void UnexportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag,
        int importRefCounter) override
    {
        DoUnexportObject(object->As<TObject>(), destinationCellTag, importRefCounter);
    }

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    TObjectTypeMetadata Metadata_;


    virtual TCellTagList DoGetReplicationCellTags(const TObject* /*object*/)
    {
        return EmptyCellTags();
    }

    virtual TString DoGetName(const TObject* object) = 0;

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
        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->FindSchema(GetType());
    }

    virtual void DoExportObject(
        TObject* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/)
    {
        Y_UNREACHABLE();
    }

    virtual void DoUnexportObject(
        TObject* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/,
        int /*importRefCounter*/)
    {
        Y_UNREACHABLE();
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
    typedef typename NHydra::TEntityMap<TObject> TMapType;

    TObjectTypeHandlerWithMapBase(NCellMaster::TBootstrap* bootstrap, TMapType* map)
        : TObjectTypeHandlerBase<TObject>(bootstrap)
        , Map_(map)
    { }

    virtual void DestroyObject(TObjectBase* object) noexcept override
    {
        this->DoDestroyObject(object->As<TObject>());
        // Remove the object from the map but keep it alive.
        Map_->Release(object->GetId()).release();
    }

    virtual NObjectServer::TObjectBase* FindObject(const TObjectId& id) override
    {
        return Map_->Find(id);
    }

protected:
    // We store map by a raw pointer. In most cases this should be OK.
    TMapType* const Map_;


    virtual void DoDestroyObject(TObject* object)
    {
        // Clear ACD, if any.
        auto* acd = this->FindAcd(object);
        if (acd) {
            acd->Clear();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

