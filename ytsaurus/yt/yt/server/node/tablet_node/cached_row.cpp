#include "cached_row.h"


namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

char* CaptureStringLikeValues(NTableClient::TMutableVersionedRow versionedRow)
{
    char* blobDataDest = const_cast<char*>(versionedRow.EndMemory());
    for (auto& value : versionedRow.Keys()) {
        if (IsStringLikeType(value.Type)) {
            memcpy(blobDataDest, value.Data.String, value.Length);
            value.Data.String = blobDataDest;
            blobDataDest += value.Length;
        }
    }

    for (auto& value : versionedRow.Values()) {
        if (IsStringLikeType(value.Type)) {
            memcpy(blobDataDest, value.Data.String, value.Length);
            value.Data.String = blobDataDest;
            blobDataDest += value.Length;
        }
    }

    return blobDataDest;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

