#include "dynamic_store_type_handler.h"
#include "dynamic_store.h"
#include "dynamic_store_proxy.h"
#include "chunk_manager.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TDynamicStoreTypeHandler
    : public TObjectTypeHandlerWithMapBase<TDynamicStore>
{
public:
    TDynamicStoreTypeHandler(TBootstrap* bootstrap, EObjectType type)
        : TObjectTypeHandlerWithMapBase(bootstrap, &bootstrap->GetChunkManager()->MutableDynamicStores())
        , Type_(type)
    { }

    EObjectType GetType() const override
    {
        return Type_;
    }

private:
    const EObjectType Type_;

    IObjectProxyPtr DoGetProxy(TDynamicStore* dynamicStore, TTransaction* /*transaction*/) override
    {
        return CreateDynamicStoreProxy(Bootstrap_, &Metadata_, dynamicStore);
    }

    void DoDestroyObject(TDynamicStore* dynamicStore) noexcept override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DestroyDynamicStore(dynamicStore);

        TObjectTypeHandlerWithMapBase::DoDestroyObject(dynamicStore);
    }

    void DoUnstageObject(TDynamicStore* /*dynamicStore*/, bool /*recursive*/) override
    {
        YT_ABORT();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateDynamicStoreTypeHandler(
    TBootstrap* bootstrap,
    EObjectType type)
{
    return New<TDynamicStoreTypeHandler>(bootstrap, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

