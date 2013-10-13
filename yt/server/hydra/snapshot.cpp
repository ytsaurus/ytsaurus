#include "stdafx.h"
#include "snapshot.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TSnapshotCreateParams::TSnapshotCreateParams()
    : PrevRecordCount(-1)
{ }

TSnapshotParams::TSnapshotParams()
    : PrevRecordCount(-1)
    , Checksum(0)
    , CompressedLength(-1)
    , UncompressedLength(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

TSnapshotParams ISnapshotStore::GetSnapshotParamsOrThrow(int snapshotId)
{
    auto result = TryGetSnapshotParams(snapshotId);
    if (!result) {
        THROW_ERROR_EXCEPTION(
            NHydra::EErrorCode::NoSuchSnapshot,
            "No such snapshot %d",
            snapshotId);
    }
    return *result;
}

ISnapshotReaderPtr ISnapshotStore::CreateReaderOrThrow(int snapshotId)
{
    auto reader = TryCreateReader(snapshotId);
    if (!reader) {
        THROW_ERROR_EXCEPTION(
            NHydra::EErrorCode::NoSuchSnapshot,
            "No such snapshot %d",
            snapshotId);
    }
    return reader;
}

ISnapshotReaderPtr ISnapshotStore::CreateRawReaderOrThrow(
    int snapshotId,
    i64 offset)
{
    auto reader = TryCreateRawReader(snapshotId, offset);
    if (!reader) {
        THROW_ERROR_EXCEPTION(
            NHydra::EErrorCode::NoSuchSnapshot,
            "No such snapshot %d",
            snapshotId);
    }
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
