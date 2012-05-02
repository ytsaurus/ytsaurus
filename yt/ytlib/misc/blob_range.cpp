#include "stdafx.h"
#include "blob_range.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TBlobRange::TBlobRange(const TBlob* blob, size_t offset, size_t length)
    : Blob(blob)
    , Offset(offset)
    , Length(length)
{ }

TStringBuf TBlobRange::GetStringBuf() const
{
    return TStringBuf(Blob->begin() + Offset, Length);
}

bool operator==(const TBlobRange& lhs, const TBlobRange& rhs)
{
    return lhs.GetStringBuf() == rhs.GetStringBuf();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
