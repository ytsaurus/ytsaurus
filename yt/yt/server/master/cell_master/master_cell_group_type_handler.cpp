#include "master_cell_group_type_handler.h"

#include "bootstrap.h"
#include "master_cell_group.h"
#include "master_cell_group_manager.h"
#include "master_cell_group_proxy.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMasterCellGroupTypeHandler
    : public TObjectTypeHandlerWithMapBase<TMasterCellGroup>
{
public:
    using TObjectTypeHandlerWithMapBase::TObjectTypeHandlerWithMapBase;

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::MasterCellGroup;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<std::string>(EInternedAttributeKey::Name.Unintern());
        auto cellTags = attributes->GetAndRemove<TCellTagSet>(EInternedAttributeKey::CellTags.Unintern(), TCellTagSet());

        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();
        return groupManager->CreateMasterCellGroup(name, cellTags, hintId);
    }

private:
    TCellTagSet DoGetReplicationCellTags(const TMasterCellGroup* /*group*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(TMasterCellGroup* group, TTransaction* /*transaction*/) override
    {
        return CreateMasterCellGroupProxy(Bootstrap_, &Metadata_, group);
    }

    void DoZombifyObject(TMasterCellGroup* group) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(group);

        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();
        groupManager->ZombifyMasterCellGroup(group);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateMasterCellGroupTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TMasterCellGroup>* map)
{
    return New<TMasterCellGroupTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
