#include "access_control_object_type_handler.h"
#include "access_control_object.h"
#include "access_control_object_proxy.h"
#include "cypress_manager.h"
#include "private.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectTypeHandler
    : public TObjectTypeHandlerWithMapBase<TAccessControlObject>
{
public:
    TAccessControlObjectTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TAccessControlObject>* map)
        : TObjectTypeHandlerWithMapBase<TAccessControlObject>(bootstrap, map)
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    TCellTagList DoGetReplicationCellTags(const TAccessControlObject*) override
    {
        return AllSecondaryCellTags();
    }

    EObjectType GetType() const override
    {
        return EObjectType::AccessControlObject;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto name = attributes->GetAndRemove<TString>(EInternedAttributeKey::Name.Unintern());
        auto namespace_ = attributes->GetAndRemove<TString>(EInternedAttributeKey::Namespace.Unintern());
        return cypressManager->CreateAccessControlObject(name, namespace_, hintId);
    }

    std::optional<TObject*> FindObjectByAttributes(
        const IAttributeDictionary* attributes) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto name = attributes->Get<TString>("name");
        auto namespace_ = attributes->Get<TString>("namespace");
        auto* namespaceObject = cypressManager->FindAccessControlObjectNamespaceByName(namespace_);
        if (!namespaceObject) {
            return nullptr;
        }

        if (auto* result = namespaceObject->FindMember(name)) {
            return result;
        }

        return nullptr;
    }

private:
    TAcdList DoListAcds(TAccessControlObject* accessControlObject) override
    {
        auto* acd = DoFindAcd(accessControlObject);
        YT_VERIFY(acd);
        return {acd, &accessControlObject->PrincipalAcd()};
    }

    TAccessControlDescriptor* DoFindAcd(TAccessControlObject* accessControlObject) override
    {
        return &accessControlObject->Acd();
    }

    IObjectProxyPtr DoGetProxy(TAccessControlObject* accessControlObject, TTransaction* /*transaction*/) override
    {
        return CreateAccessControlObjectProxy(
            Bootstrap_,
            &Metadata_,
            accessControlObject);
    }

    void DoZombifyObject(TAccessControlObject* accessControlObject) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        TObjectTypeHandlerWithMapBase::DoZombifyObject(accessControlObject);
        cypressManager->ZombifyAccessControlObject(accessControlObject);
    }

    TObject* DoGetParent(TAccessControlObject* object) override
    {
        // NB: this means that access control objects' schema is essentially useless.
        return object->Namespace().Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateAccessControlObjectTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TAccessControlObject>* map)
{
    return New<TAccessControlObjectTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NObjectServer
