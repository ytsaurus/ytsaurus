#include "medium_type_handler_base.h"

#include "chunk_manager.h"
#include "medium_base.h"

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TObject* TMediumTypeHandlerBase<TImpl>::FindObject(TObjectId id)
{
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    return chunkManager->FindMedium(id);
}

template <class TImpl>
void TMediumTypeHandlerBase<TImpl>::DoZombifyObject(TImpl* tablet) noexcept
{
    TBase::DoZombifyObject(tablet);

    // NB: Destroying arbitrary media is not currently supported.
    // This handler, however, is needed to destroy just-created media
    // for which attribute initialization has failed.
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    chunkManager->DestroyMedium(tablet);
}

template <class TImpl>
void TMediumTypeHandlerBase<TImpl>::CheckInvariants(TBootstrap* bootstrap)
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    for (auto [mediumId, medium] : chunkManager->Media()) {
        if (medium->GetType() == this->GetType()) {
            medium->CheckInvariants(bootstrap);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TMediumTypeHandlerBase<TDomesticMedium>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
