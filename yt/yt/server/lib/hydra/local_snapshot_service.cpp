#include "private.h"
#include "local_snapshot_service.h"
#include "snapshot_service_proxy.h"

#include <yt/yt/server/lib/hydra_common/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/core/rpc/service_detail.h>

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
        ILegacySnapshotStorePtr store,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            GetHydraIOInvoker(),
            TSnapshotServiceProxy::GetDescriptor(),
            HydraLogger,
            cellId,
            std::move(authenticator))
        , Store_(std::move(store))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LookupSnapshot));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadSnapshot)
            .SetCancelable(true));
    }

private:
    const ILegacySnapshotStorePtr Store_;

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
            auto asyncSnapshotId = Store_->GetLatestSnapshotId(maxSnapshotId);
            snapshotId = WaitFor(asyncSnapshotId)
                .ValueOrThrow();
            if (snapshotId == InvalidSegmentId) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchSnapshot,
                    "No appropriate snapshots in store");
            }
        }

        auto reader = Store_->CreateReader(snapshotId);

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

        YT_VERIFY(offset >= 0);
        YT_VERIFY(length >= 0);

        auto reader = Store_->CreateRawReader(snapshotId, offset);

        WaitFor(reader->Open())
            .ThrowOnError();

        auto copyingReader = CreateCopyingAdapter(reader);

        struct TSnapshotBlockTag { };
        auto buffer = TSharedMutableRef::Allocate<TSnapshotBlockTag>(length, {.InitializeStorage = false});

        auto bytesRead = WaitFor(copyingReader->Read(buffer))
            .ValueOrThrow();

        response->Attachments().push_back(buffer.Slice(0, bytesRead));

        context->SetResponseInfo("BytesRead: %v", bytesRead);
        context->Reply();
    }
};

IServicePtr CreateLocalSnapshotService(
    TCellId cellId,
    ILegacySnapshotStorePtr store,
    IAuthenticatorPtr authenticator)
{
    return New<TLocalSnapshotService>(
        cellId,
        std::move(store),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
