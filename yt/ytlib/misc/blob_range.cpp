#include "stdafx.h"
#include "blob_range.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TBlobRange::TBlobRange()
    : Blob(NULL)
    , Offset(0)
    , Length(0)
{ }


TBlobRange::TBlobRange(const TBlob* blob, size_t offset, size_t length)
    : Blob(blob)
    , Offset(offset)
    , Length(length)
{ }

TStringBuf TBlobRange::GetStringBuf() const
{
    return TStringBuf(Blob->begin() + Offset, Length);
}

TBlob::const_iterator TBlobRange::begin() const
{
    return Blob->begin() + Offset;
}

size_t TBlobRange::size() const
{
    return Length;
}


bool operator==(const TBlobRange& lhs, const TBlobRange& rhs)
{
    return lhs.GetStringBuf() == rhs.GetStringBuf();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
