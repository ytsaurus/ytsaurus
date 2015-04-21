#include "stdafx.h"
#include "plan_fragment_common.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode)
{
    switch (opcode) {
        case EUnaryOp::Plus:           return "+";
        case EUnaryOp::Minus:          return "-";
        case EUnaryOp::Not:            return "NOT";
    }
    YUNREACHABLE();
}

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return "+";
        case EBinaryOp::Minus:          return "-";
        case EBinaryOp::Multiply:       return "*";
        case EBinaryOp::Divide:         return "/";
        case EBinaryOp::Modulo:         return "%";
        case EBinaryOp::And:            return "AND";
        case EBinaryOp::Or:             return "OR";
        case EBinaryOp::Equal:          return "=";
        case EBinaryOp::NotEqual:       return "!=";
        case EBinaryOp::Less:           return "<";
        case EBinaryOp::LessOrEqual:    return "<=";
        case EBinaryOp::Greater:        return ">";
        case EBinaryOp::GreaterOrEqual: return ">=";
    }
    YUNREACHABLE();
}

EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Less:           return EBinaryOp::Greater;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::Greater:        return EBinaryOp::Less;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::LessOrEqual;
        default:                        return opcode;
    }
}

EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:          return EBinaryOp::NotEqual;
        case EBinaryOp::NotEqual:       return EBinaryOp::Equal;
        case EBinaryOp::Less:           return EBinaryOp::Greater;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::Greater:        return EBinaryOp::Less;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::LessOrEqual;
        default:                        YUNREACHABLE();
    }
}

bool IsBinaryOpCompare(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return false;
        case EBinaryOp::Minus:          return false;
        case EBinaryOp::Multiply:       return false;
        case EBinaryOp::Divide:         return false;
        case EBinaryOp::Modulo:         return false;
        case EBinaryOp::And:            return false;
        case EBinaryOp::Or:             return false;
        case EBinaryOp::Equal:          return true;
        case EBinaryOp::NotEqual:       return true;
        case EBinaryOp::Less:           return true;
        case EBinaryOp::LessOrEqual:    return true;
        case EBinaryOp::Greater:        return true;
        case EBinaryOp::GreaterOrEqual: return true;
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
