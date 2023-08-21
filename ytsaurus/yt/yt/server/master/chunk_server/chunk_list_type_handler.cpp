#include "chunk_list_type_handler.h"
#include "chunk_list.h"
#include "chunk_list_proxy.h"
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

class TChunkListTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunkList>
{
public:
    explicit TChunkListTypeHandler(TBootstrap* bootstrap)
        : TObjectTypeHandlerWithMapBase(bootstrap, &bootstrap->GetChunkManager()->MutableChunkLists())
    { }

    EObjectType GetType() const override
    {
        return EObjectType::ChunkList;
    }

private:
    IObjectProxyPtr DoGetProxy(TChunkList* chunkList, TTransaction* /*transaction*/) override
    {
        return CreateChunkListProxy(Bootstrap_, &Metadata_, chunkList);
    }

    void DoDestroyObject(TChunkList* chunkList) noexcept override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DestroyChunkList(chunkList);

        TObjectTypeHandlerWithMapBase::DoDestroyObject(chunkList);
    }

    void DoUnstageObject(TChunkList* chunkList, bool recursive) override
    {
        TObjectTypeHandlerWithMapBase::DoUnstageObject(chunkList, recursive);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->UnstageChunkList(chunkList, recursive);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChunkListTypeHandler(TBootstrap* bootstrap)
{
    return New<TChunkListTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

