#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::pair<int, int> TSourceLocation;
static const TSourceLocation NullSourceLocation(0, 0);

DECLARE_ENUM(EBinaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    (Multiply)
    (Divide)
    // Integral operations.
    (Modulo)
    // Logical operations.
    (And)
    (Or)
    // Relational operations.
    (Equal)
    (NotEqual)
    (Less)
    (LessOrEqual)
    (Greater)
    (GreaterOrEqual)
);

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode);

// Reverse binary opcode for compariosn operations.
EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
