#include "s3_medium_type_handler.h"

#include "s3_medium.h"
#include "s3_medium_proxy.h"
#include "chunk_manager.h"
#include "config.h"
#include "medium_type_handler_base.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TS3MediumTypeHandler
    : public TMediumTypeHandlerBase<TS3Medium>
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
        return EObjectType::S3Medium;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto name = attributes->GetAndRemove<TString>("name");
        auto config = attributes->GetAndRemove<TS3MediumConfigPtr>("config");
        // These three are optional.
        auto priority = attributes->FindAndRemove<int>("priority");
        auto index = attributes->FindAndRemove<int>("index");

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->CreateS3Medium(
            name,
            config,
            priority,
            index,
            hintId);
    }

private:
    IObjectProxyPtr DoGetProxy(TS3Medium* medium, TTransaction* /*transaction*/) override
    {
        return CreateS3MediumProxy(Bootstrap_, &Metadata_, medium);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateS3MediumTypeHandler(TBootstrap* bootstrap)
{
    return New<TS3MediumTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

