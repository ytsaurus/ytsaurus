#pragma once

#include "object_detail.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/server/lib/hydra/entity_map.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/core/ytree/ypath_detail.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectTypeMetadata
{
    NYTree::TSystemBuiltinAttributeKeysCache SystemBuiltinAttributeKeysCache;
    NYTree::TSystemCustomAttributeKeysCache SystemCustomAttributeKeysCache;
    NYTree::TOpaqueAttributeKeysCache OpaqueAttributeKeysCache;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    explicit TObjectTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        YT_VERIFY(bootstrap);
    }

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

    virtual TCellTagList GetReplicationCellTags(const TObject* object) override
    {
        return DoGetReplicationCellTags(object->As<TImpl>());
    }

    virtual TString GetName(const TObject* object) override
    {
        return DoGetName(object->As<TImpl>());
    }

    virtual IObjectProxyPtr GetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(object->As<TImpl>(), transaction);
    }

    virtual TObject* CreateObject(
        TObjectId /*hintId*/,
        NYTree::IAttributeDictionary* /*attributes*/) override
    {
        YT_ABORT();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(TObjectId /*id*/) override
    {
        YT_ABORT();
    }

    virtual void DestroyObject(TObject* object) noexcept override
    {
        DoDestroyObject(object->As<TImpl>());
    }

    virtual void ZombifyObject(TObject* object) noexcept override
    {
        DoZombifyObject(object->As<TImpl>());
    }

    virtual void UnstageObject(TObject* object, bool recursive) override
    {
        DoUnstageObject(object->As<TImpl>(), recursive);
    }

    virtual TObject* FindObjectByAttributes(
        const NYTree::IAttributeDictionary* /*attributes*/) override
    {
        THROW_ERROR_EXCEPTION("Searching by attributes is not supported for type %Qlv",
            GetType());
    }

    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObject* object) override
    {
        return DoFindAcd(object->As<TImpl>());
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return DoGetParent(object->As<TImpl>());
    }

    virtual void ExportObject(
        TObject* object,
        NObjectClient::TCellTag destinationCellTag) override
    {
        DoExportObject(object->As<TImpl>(), destinationCellTag);
    }

    virtual void UnexportObject(
        TObject* object,
        NObjectClient::TCellTag destinationCellTag,
        int importRefCounter) override
    {
        DoUnexportObject(object->As<TImpl>(), destinationCellTag, importRefCounter);
    }

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    TObjectTypeMetadata Metadata_;

    virtual TCellTagList DoGetReplicationCellTags(const TImpl* /*object*/)
    {
        return EmptyCellTags();
    }

    virtual TString DoGetName(const TImpl* object)
    {
        return object->GetLowercaseObjectName();
    }

    virtual IObjectProxyPtr DoGetProxy(
        TImpl* object,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual void DoDestroyObject(TImpl* object)
    {
        // Clear ACD, if any.
        auto* acd = FindAcd(object);
        if (acd) {
            acd->Clear();
        }
    }

    virtual void DoZombifyObject(TImpl* /*object*/)
    { }

    virtual void DoUnstageObject(TImpl* /*object*/, bool /*recursive*/)
    { }

    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TImpl* /*object*/)
    {
        return nullptr;
    }

    virtual TObject* DoGetParent(TImpl* /*object*/)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->FindSchema(GetType());
    }

    virtual void DoExportObject(
        TImpl* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/)
    {
        YT_ABORT();
    }

    virtual void DoUnexportObject(
        TImpl* /*object*/,
        NObjectClient::TCellTag /*destinationCellTag*/,
        int /*importRefCounter*/)
    {
        YT_ABORT();
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
    using TMapType = NHydra::TEntityMap<TObject>;
    using TBase = TObjectTypeHandlerBase<TObject>;

    TObjectTypeHandlerWithMapBase(NCellMaster::TBootstrap* bootstrap, TMapType* map)
        : TObjectTypeHandlerBase<TObject>(bootstrap)
        , Map_(map)
    { }

    virtual NObjectServer::TObject* FindObject(TObjectId id) override
    {
        return Map_->Find(id);
    }

protected:
    // We store map by a raw pointer. In most cases this should be OK.
    TMapType* const Map_;


    virtual void DoDestroyObject(TObject* object) override
    {
        TBase::DoDestroyObject(object);
        // Remove the object from the map but keep it alive.
        Map_->Release(object->NObjectServer::TObject::GetId()).release();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

