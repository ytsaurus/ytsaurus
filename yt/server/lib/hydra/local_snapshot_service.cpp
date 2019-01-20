#include "local_snapshot_service.h"
#include "private.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "snapshot_service_proxy.h"

#include <yt/core/rpc/service_detail.h>

namespace NYT::NHydra {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotService
    : public NRpc::TServiceBase
{
public:
    TLocalSnapshotService(
        TCellId cellId,
        TFileSnapshotStorePtr fileStore)
        : TServiceBase(
            GetHydraIOInvoker(),
            TSnapshotServiceProxy::GetDescriptor(),
            HydraLogger,
            cellId)
        , FileStore_(std::move(fileStore))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot)
            .SetCancelable(true));
    }

private:
    const TFileSnapshotStorePtr FileStore_;

    DECLARE_RPC_SERVICE_METHOD(NProto, LookupSnapshot)
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
            if (snapshotId == InvalidSegmentId) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchSnapshot,
                    "No appropriate snapshots in store");
            }
        }

        auto reader = FileStore_->CreateReader(snapshotId);

        WaitFor(reader->Open())
            .ThrowOnError();

        auto params = reader->GetParams();
        response->set_snapshot_id(snapshotId);
        response->set_compressed_length(params.CompressedLength);
        response->set_uncompressed_length(params.UncompressedLength);
        response->set_checksum(params.Checksum);
        *response->mutable_meta() = params.Meta;

        context->SetResponseInfo("SnapshotId: %v", snapshotId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReadSnapshot)
    {
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

        WaitFor(reader->Open())
            .ThrowOnError();

        auto copyingReader = CreateCopyingAdapter(reader);

        struct TSnapshotBlockTag { };
        auto buffer = TSharedMutableRef::Allocate<TSnapshotBlockTag>(length, false);

        auto bytesRead = WaitFor(copyingReader->Read(buffer))
            .ValueOrThrow();

        response->Attachments().push_back(buffer.Slice(0, bytesRead));

        context->SetResponseInfo("BytesRead: %v", bytesRead);
        context->Reply();
    }
};

IServicePtr CreateLocalSnapshotService(
    TCellId cellId,
    TFileSnapshotStorePtr fileStore)
{
    return New<TLocalSnapshotService>(
        cellId,
        std::move(fileStore));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
