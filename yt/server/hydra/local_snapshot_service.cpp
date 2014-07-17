#include "stdafx.h"
#include "local_snapshot_service.h"
#include "snapshot_service_proxy.h"
#include "snapshot.h"
#include "file_snapshot_store.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

TLocalSnapshotService::TLocalSnapshotService(
    const TCellGuid& cellGuid,
    TFileSnapshotStorePtr fileStore)
    : TServiceBase(
        GetHydraIOInvoker(),
        TServiceId(TSnapshotServiceProxy::GetServiceName(), cellGuid),
        HydraLogger)
    , FileStore_(fileStore)
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot));
}

DEFINE_RPC_SERVICE_METHOD(TLocalSnapshotService, LookupSnapshot)
{
    int maxSnapshotId = request->max_snapshot_id();
    bool exactId = request->exact_id();

    context->SetRequestInfo("MaxSnapshotId: %v, ExactId: %v",
        maxSnapshotId,
        exactId);

    int snapshotId;
    if (exactId) {
        snapshotId = maxSnapshotId;
    } else {
        snapshotId = FileStore_->GetLatestSnapshotId(maxSnapshotId);
        if (snapshotId == NonexistingSegmentId) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::NoSuchSnapshot,
                "No appropriate snapshots in store");
        }
    }

    auto maybeParams = FileStore_->FindSnapshotParams(snapshotId);
    if (!maybeParams) {
        THROW_ERROR_EXCEPTION("No such snapshot %v", snapshotId);
    }

    response->set_snapshot_id(snapshotId);
    response->set_prev_record_count(maybeParams->PrevRecordCount);
    response->set_compressed_length(maybeParams->CompressedLength);
    response->set_uncompressed_length(maybeParams->UncompressedLength);
    response->set_checksum(maybeParams->Checksum);
    
    context->SetResponseInfo("SnapshotId: %v", snapshotId);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TLocalSnapshotService, ReadSnapshot)
{
    UNUSED(response);

    int snapshotId = request->snapshot_id();
    i64 offset = request->offset();
    i64 length = request->length();

    context->SetRequestInfo("SnapshotId: %v, Offset: %v, Length: %v",
        snapshotId,
        offset,
        length);

    YCHECK(offset >= 0);
    YCHECK(length >= 0);

    auto reader = FileStore_->CreateRawReader(snapshotId, offset);

    struct TSnapshotBlockTag { };
    auto buffer = TSharedRef::Allocate<TSnapshotBlockTag>(length, false);
    size_t bytesRead = reader->GetStream()->Read(buffer.Begin(), length);
    auto data = buffer.Slice(TRef(buffer.Begin(), bytesRead));
    context->Response().Attachments().push_back(data);

    context->SetResponseInfo("BytesRead: %v", bytesRead);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
