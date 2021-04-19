#include "cached_row.h"


namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

char* CaptureStringLikeValues(NTableClient::TMutableVersionedRow versionedRow)
{
    char* blobDataDest = const_cast<char*>(versionedRow.GetMemoryEnd());
    for (auto it = versionedRow.BeginKeys(); it != versionedRow.EndKeys(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(blobDataDest, it->Data.String, it->Length);
            it->Data.String = blobDataDest;
            blobDataDest += it->Length;
        }
    }

    for (auto it = versionedRow.BeginValues(); it != versionedRow.EndValues(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(blobDataDest, it->Data.String, it->Length);
            it->Data.String = blobDataDest;
            blobDataDest += it->Length;
        }
    }

    return blobDataDest;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

