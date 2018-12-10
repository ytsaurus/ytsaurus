#include "query_common.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode)
{
    switch (opcode) {
        case EUnaryOp::Plus:  return "+";
        case EUnaryOp::Minus: return "-";
        case EUnaryOp::Not:   return "NOT";
        case EUnaryOp::BitNot:return "~";
        default:              Y_UNREACHABLE();
    }
}

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return "+";
        case EBinaryOp::Minus:          return "-";
        case EBinaryOp::Multiply:       return "*";
        case EBinaryOp::Divide:         return "/";
        case EBinaryOp::Modulo:         return "%";
        case EBinaryOp::LeftShift:      return "<<";
        case EBinaryOp::RightShift:     return ">>";
        case EBinaryOp::BitAnd:         return "&";
        case EBinaryOp::BitOr:          return "|";
        case EBinaryOp::And:            return "AND";
        case EBinaryOp::Or:             return "OR";
        case EBinaryOp::Equal:          return "=";
        case EBinaryOp::NotEqual:       return "!=";
        case EBinaryOp::Less:           return "<";
        case EBinaryOp::LessOrEqual:    return "<=";
        case EBinaryOp::Greater:        return ">";
        case EBinaryOp::GreaterOrEqual: return ">=";
        default:                        Y_UNREACHABLE();
    }
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
        case EBinaryOp::Less:           return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::Greater;
        case EBinaryOp::Greater:        return EBinaryOp::LessOrEqual;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::Less;
        default:                        Y_UNREACHABLE();
    }
}

bool IsArithmeticalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            return true;
        default:
            return false;
    }
}

bool IsIntegralBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            return true;
        default:
            return false;
    }
}

bool IsLogicalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::And:
        case EBinaryOp::Or:
            return true;
        default:
            return false;
    }
}

bool IsRelationalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return true;
        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
