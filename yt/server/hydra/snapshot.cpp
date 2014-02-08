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

} // namespace NHydra
} // namespace NYT
