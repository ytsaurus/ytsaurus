#include "format.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

const ui64 TChangelogHeader::ExpectedSignature;
const ui64 TChangelogHeader::ExpectedSignatureOld; // COMPAT(aozeritsky): old format
const ui64 TChangelogIndexHeader::ExpectedSignature;
const ui64 TChangelogIndexHeader::ExpectedSignatureOld; // COMPAT(aozeritsky): old format
const ui64 TSnapshotHeader::ExpectedSignature;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
