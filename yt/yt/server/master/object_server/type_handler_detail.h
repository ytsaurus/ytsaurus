#pragma once

#include "object_detail.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/ytree/ypath_detail.h>

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

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

    TCellTagList GetReplicationCellTags(const TObject* object) override
    {
        return DoGetReplicationCellTags(object->As<TImpl>());
    }

    TString GetName(const TObject* object) override
    {
        return DoGetName(object->As<TImpl>());
    }

    TString GetPath(const TObject* object) override
    {
        return DoGetPath(object->As<TImpl>());
    }

    IObjectProxyPtr GetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(object->As<TImpl>(), transaction);
    }

    TObject* CreateObject(
        TObjectId /*hintId*/,
        NYTree::IAttributeDictionary* /*attributes*/) override
    {
        YT_ABORT();
    }

    void DestroyObject(TObject* object) noexcept override
    {
        DoDestroyObject(object->As<TImpl>());
    }

    void DestroySequoiaObject(TObject* object, const NSequoiaClient::ISequoiaTransactionPtr& transaction) noexcept override
    {
        DoDestroySequoiaObject(object->As<TImpl>(), transaction);
    }

    void RecreateObjectAsGhost(TObject* object) noexcept override
    {
        DoRecreateObjectAsGhost(object->As<TImpl>());
    }

    void ZombifyObject(TObject* object) noexcept override
    {
        DoZombifyObject(object->As<TImpl>());
    }

    void UnstageObject(TObject* object, bool recursive) override
    {
        DoUnstageObject(object->As<TImpl>(), recursive);
    }

    std::optional<TObject*> FindObjectByAttributes(
        const NYTree::IAttributeDictionary* /*attributes*/) override
    {
        return std::nullopt;
    }

    NSecurityServer::TAccessControlDescriptor* FindAcd(TObject* object) override
    {
        return DoFindAcd(object->As<TImpl>());
    }

    TAcdList ListAcds(TObject* object) override
    {
        return DoListAcds(object->As<TImpl>());
    }

    std::optional<std::vector<TString>> ListColumns(TObject* object) override
    {
        return DoListColumns(object->As<TImpl>());
    }

    TObject* GetParent(TObject* object) override
    {
        return DoGetParent(object->As<TImpl>());
    }

    void ExportObject(
        TObject* object,
        NObjectClient::TCellTag destinationCellTag) override
    {
        DoExportObject(object->As<TImpl>(), destinationCellTag);
    }

    void UnexportObject(
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

    virtual TString DoGetPath(const TImpl* object)
    {
        return object->GetObjectPath();
    }

    virtual IObjectProxyPtr DoGetProxy(
        TImpl* object,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual void DoDestroyObject(TImpl* object) noexcept
    {
        // Clear ACD, if any.
        for (auto* acd : ListAcds(object)) {
            YT_VERIFY(acd);
            acd->Clear();
        }
    }

    virtual void DoDestroySequoiaObject(TImpl* /*object*/, const NSequoiaClient::ISequoiaTransactionPtr& /*transaction*/)
    { }

    virtual void DoRecreateObjectAsGhost(TImpl* object) noexcept = 0;

    virtual void DoZombifyObject(TImpl* /*object*/)
    { }

    virtual void DoUnstageObject(TImpl* /*object*/, bool /*recursive*/)
    { }

    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TImpl* /*object*/)
    {
        return nullptr;
    }

    virtual TAcdList DoListAcds(TImpl* object)
    {
        if (auto* acd = DoFindAcd(object)) {
            return TAcdList{acd};
        }
        return {};
    }

    virtual std::optional<std::vector<TString>> DoListColumns(TImpl* /*object*/)
    {
        return std::nullopt;
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

template <class TImpl>
class TConcreteObjectTypeHandlerBase
    : public TObjectTypeHandlerBase<TImpl>
{
public:
    using TObjectTypeHandlerBase<TImpl>::TObjectTypeHandlerBase;

    std::unique_ptr<TObject> InstantiateObject(TObjectId id) override
    {
        return TPoolAllocator::New<TImpl>(id);
    }

protected:
    void DoRecreateObjectAsGhost(TImpl* object) noexcept override
    {
        TObject::RecreateAsGhost(object);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerWithMapBase
    : public TConcreteObjectTypeHandlerBase<TObject>
{
public:
    using TMapType = NHydra::TEntityMap<TObject>;
    using TBase = TObjectTypeHandlerBase<TObject>;

    TObjectTypeHandlerWithMapBase(NCellMaster::TBootstrap* bootstrap, TMapType* map)
        : TConcreteObjectTypeHandlerBase<TObject>(bootstrap)
        , Map_(map)
    { }

    NObjectServer::TObject* FindObject(TObjectId id) override
    {
        return Map_->Find(id);
    }

protected:
    // We store map by a raw pointer. In most cases this should be OK.
    TMapType* const Map_;


    void DoDestroyObject(TObject* object) noexcept override
    {
        // Remove the object from the map but keep it alive.
        Map_->Release(object->NObjectServer::TObject::GetId()).release();

        TBase::DoDestroyObject(object);
    }

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) override
    {
        for (auto [objectId, object] : *Map_) {
            object->CheckInvariants(bootstrap);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
