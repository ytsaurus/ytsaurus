#include "chunk_view_type_handler.h"
#include "chunk_view.h"
#include "chunk_view_proxy.h"
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

class TChunkViewTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunkView>
{
public:
    explicit TChunkViewTypeHandler(TBootstrap* bootstrap)
        : TObjectTypeHandlerWithMapBase(bootstrap, &bootstrap->GetChunkManager()->MutableChunkViews())
    { }

    EObjectType GetType() const override
    {
        return EObjectType::ChunkView;
    }

private:
    IObjectProxyPtr DoGetProxy(TChunkView* chunkView, TTransaction* /*transaction*/) override
    {
        return CreateChunkViewProxy(Bootstrap_, &Metadata_, chunkView);
    }

    void DoDestroyObject(TChunkView* chunkView) noexcept override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DestroyChunkView(chunkView);

        TObjectTypeHandlerWithMapBase::DoDestroyObject(chunkView);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChunkViewTypeHandler(TBootstrap* bootstrap)
{
    return New<TChunkViewTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

