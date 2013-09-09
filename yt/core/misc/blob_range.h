#pragma once

#include "common.h"
#include "ref.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

//! A (non-owning) pointer to a const TBlob instance plus a range inside it (represented
//! by start offset and size).
/*!
 *  TBlobRange provides a reallocation-tolerant way of keeping references inside TBlob.
 */
class TBlobRange
{
public:
    TBlobRange();
    TBlobRange(const TBlob* blob, size_t offset, size_t size);

    TStringBuf ToStringBuf() const;

    // TODO(babenko): add more stuff when needed.
    // TODO(babenko): consider adding FORCED_INLINE 
    const char* begin() const;
    size_t size() const;

private:
    const TBlob* Blob;
    size_t Offset;
    size_t Size;

};

// TODO(babenko): add more stuff when needed.
bool operator == (const TBlobRange& lhs, const TBlobRange& rhs);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

//! A hasher for TBlobRange.
template <>
struct hash<NYT::TBlobRange>
{
    size_t operator()(const NYT::TBlobRange& value) const
    {
        return THash<TStringBuf>()(value.ToStringBuf());
    }
};
