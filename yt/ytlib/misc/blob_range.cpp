#include "stdafx.h"
#include "blob_range.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TBlobRange::TBlobRange()
    : Blob(NULL)
    , Offset(0)
    , Size(0)
{ }

TBlobRange::TBlobRange(const TBlob* blob, size_t offset, size_t length)
    : Blob(blob)
    , Offset(offset)
    , Size(length)
{ }

TStringBuf TBlobRange::ToStringBuf() const
{
    return TStringBuf(Blob->Begin() + Offset, Size);
}

const char* TBlobRange::begin() const
{
    return Blob->Begin() + Offset;
}

size_t TBlobRange::size() const
{
    return Size;
}

bool operator==(const TBlobRange& lhs, const TBlobRange& rhs)
{
    return lhs.ToStringBuf() == rhs.ToStringBuf();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
