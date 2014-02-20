#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TSourceLocation
{
    TSourceLocation();

    int GetOffset() const;
    int GetLength() const;

    // Naming is to confirm to Bison interface.
    int begin;
    int end;
};

Stroka ToString(const TSourceLocation& location);

extern const TSourceLocation NullSourceLocation;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

