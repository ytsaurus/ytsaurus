#include "medium_type_handler_base.h"

#include "chunk_manager.h"
#include "medium_base.h"
#include "s3_medium.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
TObject* TMediumTypeHandlerBase<TImpl>::FindObject(TObjectId id)
{
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    return chunkManager->FindMedium(id);
}

template <class TImpl>
void TMediumTypeHandlerBase<TImpl>::DoZombifyObject(TImpl* medium) noexcept
{
    TBase::DoZombifyObject(medium);

    // NB: Destroying arbitrary media is not currently supported.
    // This handler, however, is needed to destroy just-created media
    // for which attribute initialization has failed.
    const auto& chunkManager = TBase::Bootstrap_->GetChunkManager();
    chunkManager->DestroyMedium(medium);
}

template <class TImpl>
TCellTagList TMediumTypeHandlerBase<TImpl>::DoGetReplicationCellTags(const TImpl* /*medium*/)
{
    return TConcreteObjectTypeHandlerBase<TImpl>::AllSecondaryCellTags();
}

template <class TImpl>
TAccessControlDescriptor* TMediumTypeHandlerBase<TImpl>::DoFindAcd(TImpl* medium)
{
    return &medium->Acd();
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
template class TMediumTypeHandlerBase<TS3Medium>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
