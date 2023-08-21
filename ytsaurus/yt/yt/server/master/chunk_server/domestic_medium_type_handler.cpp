#include "domestic_medium_type_handler.h"

#include "domestic_medium.h"
#include "domestic_medium_proxy.h"
#include "medium_type_handler_base.h"
#include "chunk_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TDomesticMediumTypeHandler
    : public TMediumTypeHandlerBase<TDomesticMedium>
{
public:
    using TMediumTypeHandlerBase::TMediumTypeHandlerBase;

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
        return chunkManager->CreateDomesticMedium(
            name,
            transient,
            priority,
            index,
            hintId);
    }

private:
    IObjectProxyPtr DoGetProxy(TDomesticMedium* medium, TTransaction* /*transaction*/) override
    {
        return CreateDomesticMediumProxy(Bootstrap_, &Metadata_, medium);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateDomesticMediumTypeHandler(TBootstrap* bootstrap)
{
    return New<TDomesticMediumTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

