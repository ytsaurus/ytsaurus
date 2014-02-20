#include "stdafx.h"
#include "source_location.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TSourceLocation::TSourceLocation()
    : begin(0)
    , end(0)
{ }

int TSourceLocation::GetOffset() const
{
    return begin;
}

int TSourceLocation::GetLength() const
{
    return end - begin;
}

Stroka ToString(const TSourceLocation& location)
{
    return Sprintf("%d-%d",
        location.begin,
        location.end - 1);
}

const TSourceLocation NullSourceLocation;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

