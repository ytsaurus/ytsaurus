#include "helpers.h"

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NObjectClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void AddCellTagToSyncWith(const IClientRequestPtr& request, TCellTag cellTag)
{
    auto* ext = request->Header().MutableExtension(NProto::TMulticellSyncExt::multicell_sync_ext);
    ext->add_cell_tags_to_sync_with(ToProto(cellTag));
}

void AddCellTagToSyncWith(const IClientRequestPtr& request, TObjectId objectId)
{
    if (objectId) {
        AddCellTagToSyncWith(request, CellTagFromId(objectId));
    }
}

bool GetSuppressUpstreamSync(const NRpc::NProto::TRequestHeader& requestHeader)
{
    const auto& ext = requestHeader.GetExtension(NProto::TMulticellSyncExt::multicell_sync_ext);
    return ext.suppress_upstream_sync();
}

bool GetSuppressTransactionCoordinatorSync(const NRpc::NProto::TRequestHeader& requestHeader)
{
    const auto& ext = requestHeader.GetExtension(NProto::TMulticellSyncExt::multicell_sync_ext);
    return ext.suppress_transaction_coordinator_sync();
}

void SetSuppressUpstreamSync(NRpc::NProto::TRequestHeader* requestHeader, bool value)
{
    auto* ext = requestHeader->MutableExtension(NProto::TMulticellSyncExt::multicell_sync_ext);
    ext->set_suppress_upstream_sync(value);
}

void SetSuppressTransactionCoordinatorSync(NRpc::NProto::TRequestHeader* requestHeader, bool value)
{
    auto* ext = requestHeader->MutableExtension(NProto::TMulticellSyncExt::multicell_sync_ext);
    ext->set_suppress_transaction_coordinator_sync(value);
}

bool IsRetriableObjectServiceError(int /*attempt*/, const TError& error)
{
    // COMPAT(kvk1920): drop it when SequoiaRetriableError will be used
    // everywhere.
    if (error.FindMatching(NRpc::EErrorCode::TransientFailure)) {
        return true;
    }

    return
        error.FindMatching(NTabletClient::EErrorCode::InvalidTabletState) ||
        error.FindMatching(NChunkClient::EErrorCode::OptimisticLockFailure) ||
        error.FindMatching(NSequoiaClient::EErrorCode::SequoiaRetriableError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

