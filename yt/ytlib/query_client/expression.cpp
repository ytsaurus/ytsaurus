#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_context.h"

#include <yt/ytlib/query_client/expression.pb.h>

#include <yt/ytlib/new_table_client/unversioned_row.h>

#include <core/misc/protobuf_helpers.h>

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch-enum"
#define ENSURE_ALL_CASES
#else
#define ENSURE_ALL_CASES default: YUNREACHABLE();
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;
using NYT::FromProto;

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
        ENSURE_ALL_CASES
    }
    YUNREACHABLE();
}

EBinaryOpKind GetBinaryOpcodeKind(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            return EBinaryOpKind::Arithmetical;
        case EBinaryOp::Modulo:
            return EBinaryOpKind::Integral;
        case EBinaryOp::And:
        case EBinaryOp::Or:
            return EBinaryOpKind::Logical;
        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return EBinaryOpKind::Relational;
        ENSURE_ALL_CASES
    }
    YUNREACHABLE();
}

Stroka TExpression::GetSource() const
{
    auto source = Context_->GetSource();

    if (!source.empty()) {
        auto offset = SourceLocation_.GetOffset();
        auto length = SourceLocation_.GetLength();

        source = source.substr(offset, length);
    }

    return source;
}

EValueType TExpression::GetType(const TTableSchema& sourceSchema) const
{
    return InferType(this, sourceSchema);
}

Stroka TExpression::GetName() const
{
    return InferName(this);
}

bool TExpression::IsConstant() const
{
    return ::NYT::NQueryClient::IsConstant(this);
}

TValue TExpression::GetConstantValue() const
{
    return ::NYT::NQueryClient::GetConstantValue(this);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TExpression* original)
{
    serialized->set_kind(original->GetKind());
    switch (original->GetKind()) {

        case EExpressionKind::IntegerLiteral: {
            auto* expr = original->As<TIntegerLiteralExpression>();
            auto* proto = serialized->MutableExtension(NProto::TIntegerLiteralExpression::integer_literal_expression);
            proto->set_value(expr->GetValue());
            break;
        }

        case EExpressionKind::DoubleLiteral: {
            auto* expr = original->As<TDoubleLiteralExpression>();
            auto* proto = serialized->MutableExtension(NProto::TDoubleLiteralExpression::double_literal_expression);
            proto->set_value(expr->GetValue());
            break;
        }

        case EExpressionKind::Reference: {
            auto* expr = original->As<TReferenceExpression>();
            auto* proto = serialized->MutableExtension(NProto::TReferenceExpression::reference_expression);
            proto->set_column_name(expr->GetColumnName());
            break;
        }

        case EExpressionKind::Function: {
            auto* expr = original->As<TFunctionExpression>();
            auto* proto = serialized->MutableExtension(NProto::TFunctionExpression::function_expression);
            proto->set_function_name(expr->GetFunctionName());
            ToProto(proto->mutable_arguments(), expr->Arguments());
            break;
        }

        case EExpressionKind::BinaryOp: {
            auto* expr = original->As<TBinaryOpExpression>();
            auto* proto = serialized->MutableExtension(NProto::TBinaryOpExpression::binary_op_expression);
            proto->set_opcode(expr->GetOpcode());
            ToProto(proto->mutable_lhs(), expr->GetLhs());
            ToProto(proto->mutable_rhs(), expr->GetRhs());
            break;
        }

        ENSURE_ALL_CASES
    }
}

const TExpression* FromProto(const NProto::TExpression& serialized, TPlanContext* context)
{
    TExpression* result = nullptr;

    switch (EExpressionKind(serialized.kind())) {

        case EExpressionKind::IntegerLiteral: {
            auto data = serialized.GetExtension(NProto::TIntegerLiteralExpression::integer_literal_expression);
            auto typedResult = new (context) TIntegerLiteralExpression(
                context,
                NullSourceLocation,
                data.value());
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EExpressionKind::DoubleLiteral: {
            auto data = serialized.GetExtension(NProto::TDoubleLiteralExpression::double_literal_expression);
            auto typedResult = new (context) TDoubleLiteralExpression(
                context,
                NullSourceLocation,
                data.value());
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EExpressionKind::Reference: {
            auto data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            auto typedResult = new (context) TReferenceExpression(
                context,
                NullSourceLocation,
                data.column_name());
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EExpressionKind::Function: {
            auto data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            auto typedResult = new (context) TFunctionExpression(
                context,
                NullSourceLocation,
                data.function_name());
            typedResult->Arguments().reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments().push_back(
                    FromProto(data.arguments(i), context));
            }
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EExpressionKind::BinaryOp: {
            auto data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            auto typedResult = new (context) TBinaryOpExpression(
                context,
                NullSourceLocation,
                EBinaryOp(data.opcode()),
                FromProto(data.lhs(), context),
                FromProto(data.rhs(), context));
            YASSERT(!result);
            result = typedResult;
            break;
        }

        ENSURE_ALL_CASES
    }

    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
