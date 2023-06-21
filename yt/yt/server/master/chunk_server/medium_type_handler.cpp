#include "medium_type_handler.h"
#include "medium.h"
#include "medium_proxy.h"
#include "chunk_manager.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TMediumTypeHandler
    : public TObjectTypeHandlerWithMapBase<TMedium>
{
public:
    explicit TMediumTypeHandler(TBootstrap* bootstrap)
        : TObjectTypeHandlerWithMapBase(
            bootstrap,
            &bootstrap->GetChunkManager()->MutableMedia())
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::Medium;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        // These three are optional.
        auto priority = attributes->FindAndRemove<int>("priority");
        auto transient = attributes->FindAndRemove<bool>("transient");
        auto index = attributes->FindAndRemove<int>("index");

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->CreateMedium(name, transient, priority, index, hintId);
    }

private:
    TCellTagList DoGetReplicationCellTags(const TMedium* /*medium*/) override
    {
        return AllSecondaryCellTags();
    }

    TAccessControlDescriptor* DoFindAcd(TMedium* medium) override
    {
        return &medium->Acd();
    }

    IObjectProxyPtr DoGetProxy(TMedium* medium, TTransaction* /*transaction*/) override
    {
        return CreateMediumProxy(Bootstrap_, &Metadata_, medium);
    }

    void DoZombifyObject(TMedium* medium) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(medium);
        // NB: Destroying arbitrary media is not currently supported.
        // This handler, however, is needed to destroy just-created media
        // for which attribute initialization has failed.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DestroyMedium(medium);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateMediumTypeHandler(TBootstrap* bootstrap)
{
    return New<TMediumTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

