#include "stdafx.h"
#include "local_snapshot_service.h"
#include "snapshot_service_proxy.h"
#include "snapshot.h"
#include "file_snapshot_store.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/rpc/service_detail.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TLocalSnapshotService::TLocalSnapshotService(
    const TCellId& cellId,
    TFileSnapshotStorePtr fileStore)
    : TServiceBase(
        GetHydraIOInvoker(),
        TServiceId(TSnapshotServiceProxy::GetServiceName(), cellId),
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

    auto reader = FileStore_->CreateReader(snapshotId);

    {
        auto result = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    auto params = reader->GetParams();
    response->set_snapshot_id(snapshotId);
    response->set_compressed_length(params.CompressedLength);
    response->set_uncompressed_length(params.UncompressedLength);
    response->set_checksum(params.Checksum);
    response->set_meta(ToString(params.Meta));
    
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

    {
        auto result = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    struct TSnapshotBlockTag { };
    auto buffer = TSharedRef::Allocate<TSnapshotBlockTag>(length, false);
    auto result = WaitFor(reader->Read(buffer.Begin(), length));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
    auto bytesRead = result.Value();
    context->Response().Attachments().push_back(buffer.Slice(TRef(buffer.Begin(), bytesRead)));
    context->SetResponseInfo("BytesRead: %v", bytesRead);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
