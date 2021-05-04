#include "helpers.h"

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NObjectClient {

using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void AddCellTagToSyncWith(const IClientRequestPtr& request, TCellTag cellTag)
{
    auto* ext = request->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
    ext->add_cell_tags_to_sync_with(cellTag);
}

void AddCellTagToSyncWith(const IClientRequestPtr& request, TObjectId objectId)
{
    if (objectId) {
        AddCellTagToSyncWith(request, CellTagFromId(objectId));
    }
}

bool IsRetriableObjectServiceError(int /*attempt*/, const TError& error)
{
    return
        error.FindMatching(NTabletClient::EErrorCode::InvalidTabletState) ||
        error.FindMatching(NChunkClient::EErrorCode::OptimisticLockFailure);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

