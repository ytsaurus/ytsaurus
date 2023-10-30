#include "chunk_type_handler.h"
#include "chunk.h"
#include "chunk_proxy.h"
#include "chunk_manager.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NSequoiaClient;

////////////////////////////////////////////////////////////////////////////////

class TChunkTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunk>
{
public:
    TChunkTypeHandler(TBootstrap* bootstrap, EObjectType type)
        : TObjectTypeHandlerWithMapBase(bootstrap, &bootstrap->GetChunkManager()->MutableChunks())
        , Type_(type)
    { }

    TObject* FindObject(TObjectId id) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->Chunks().Find(DecodeChunkId(id).Id);
    }

    EObjectType GetType() const override
    {
        return Type_;
    }

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) override
    {
        // We want to make sure that invariants are checked only once for each chunk,
        // and due to the fact that there are several Chunk Type Handlers, we pick one to initiate checks for all chunks.
        if (Type_ == EObjectType::Chunk) {
            TObjectTypeHandlerWithMapBase::CheckInvariants(bootstrap);
        }
    }

private:
    const EObjectType Type_;

    IObjectProxyPtr DoGetProxy(TChunk* chunk, TTransaction* /*transaction*/) override
    {
        return CreateChunkProxy(Bootstrap_, &Metadata_, chunk);
    }

    void DoDestroyObject(TChunk* chunk) noexcept override
    {
        // NB: TObjectTypeHandlerWithMapBase::DoDestroyObject will release
        // the runtime data; postpone its call.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->DestroyChunk(chunk);

        TObjectTypeHandlerWithMapBase::DoDestroyObject(chunk);
    }

    void DoUnstageObject(TChunk* chunk, bool recursive) override
    {
        TObjectTypeHandlerWithMapBase::DoUnstageObject(chunk, recursive);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->UnstageChunk(chunk);
    }

    void DoExportObject(TChunk* chunk, TCellTag destinationCellTag) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ExportChunk(chunk, destinationCellTag);
    }

    void DoUnexportObject(TChunk* chunk, TCellTag destinationCellTag, int importRefCounter) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->UnexportChunk(chunk, destinationCellTag, importRefCounter);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChunkTypeHandler(
    TBootstrap* bootstrap,
    EObjectType type)
{
    return New<TChunkTypeHandler>(bootstrap, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
