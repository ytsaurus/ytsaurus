#pragma once

#include "public.h"

#include "query_context.h"
#include "stubs.h"

#include <core/misc/property.h>
#include <core/misc/small_vector.h>

#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

struct IAstVisitor;

struct TSourceLocation
{
    int GetOffset() const
    {
        return begin;
    }

    int GetLength() const
    {
        return end - begin;
    }

    // Naming is to confirm to Bison interface.
    int begin;
    int end;
};

extern const TSourceLocation NullSourceLocation;

} // namespace NQueryClient
} // namespace NYT

#define AST_H_
#include "expression.h"
#include "operator.h"
#undef AST_H_

