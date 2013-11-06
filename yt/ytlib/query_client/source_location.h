#pragma once

#include <util/string/printf.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TSourceLocation
{
    inline int GetOffset() const
    {
        return begin;
    }

    inline int GetLength() const
    {
        return end - begin;
    }

    Stroka ToString() const
    {
        return Sprintf("%d:%d", begin, end);
    }

    // Naming is to confirm to Bison interface.
    int begin;
    int end;
};

// Instatiated in parser.yy.
extern const TSourceLocation NullSourceLocation;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

