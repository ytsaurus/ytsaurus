#include "access_control_object_namespace_type_handler.h"
#include "access_control_object_namespace.h"
#include "access_control_object_proxy.h"
#include "cypress_manager.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectNamespaceTypeHandler
    : public TObjectTypeHandlerWithMapBase<TAccessControlObjectNamespace>
{
public:
    TAccessControlObjectNamespaceTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TAccessControlObjectNamespace>* map)
        : TObjectTypeHandlerWithMapBase<TAccessControlObjectNamespace>(bootstrap, map)
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

    TCellTagList DoGetReplicationCellTags(const TAccessControlObjectNamespace*) override
    {
        return AllSecondaryCellTags();
    }

    EObjectType GetType() const override
    {
        return EObjectType::AccessControlObjectNamespace;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto name = attributes->GetAndRemove<TString>(EInternedAttributeKey::Name.Unintern());
        return cypressManager->CreateAccessControlObjectNamespace(name, hintId);
    }

    std::optional<TObject*> FindObjectByAttributes(
        const IAttributeDictionary* attributes) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        auto name = attributes->Get<TString>(EInternedAttributeKey::Name.Unintern());
        return cypressManager->FindAccessControlObjectNamespaceByName(name);
    }

private:
    TAccessControlDescriptor* DoFindAcd(TAccessControlObjectNamespace* accessControlObjectNamespace) override
    {
        return &accessControlObjectNamespace->Acd();
    }

    IObjectProxyPtr DoGetProxy(TAccessControlObjectNamespace* accessControlObjectNamespace, TTransaction* /*transaction*/) override
    {
        return CreateAccessControlObjectNamespaceProxy(
            Bootstrap_,
            &Metadata_,
            accessControlObjectNamespace);
    }

    void DoZombifyObject(TAccessControlObjectNamespace* accessControlObjectNamespace) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        TObjectTypeHandlerWithMapBase::DoZombifyObject(accessControlObjectNamespace);
        cypressManager->ZombifyAccessControlObjectNamespace(accessControlObjectNamespace);
    }
};

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectTypeHandlerPtr CreateAccessControlObjectNamespaceTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TAccessControlObjectNamespace>* map)
{
    return New<TAccessControlObjectNamespaceTypeHandler>(bootstrap, map);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
