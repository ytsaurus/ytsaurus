#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_context.h"

#include <yt/ytlib/query_client/expression.pb.h>

#include <core/misc/protobuf_helpers.h>

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch-enum"
#define ENSURE_ALL_CASES
#else
#define ENSURE_ALL_CASES default: YUNREACHABLE()
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

Stroka TExpression::GetSource() const
{
    auto debugInformation = Context_->GetDebugInformation();

    if (debugInformation) {
        const auto& fullSource = debugInformation->Source;

        auto offset = SourceLocation_.GetOffset();
        auto length = SourceLocation_.GetLength();

        return fullSource.substr(offset, length);
    } else {
        return Stroka();
    }
}

EValueType TExpression::GetType() const
{
    return InferType(this);
}

Stroka TExpression::GetName() const
{
    return InferName(this);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TExpression* original)
{
    serialized->set_kind(original->GetKind());

    auto&& cachedType = original->GetCachedType();
    if (cachedType != EValueType::Null) {
        serialized->set_cached_type(std::move(cachedType));
    }

    auto&& cachedName = original->GetCachedName();
    if (!cachedName.empty()) {
        serialized->set_cached_name(std::move(cachedName));
    }

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
            proto->set_table_index(expr->GetTableIndex());
            proto->set_column_name(expr->GetColumnName());
            proto->set_cached_key_index(expr->GetCachedKeyIndex());
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
                data.table_index(),
                data.column_name());
            typedResult->SetCachedKeyIndex(data.cached_key_index());
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

    if (serialized.has_cached_type()) {
        result->SetCachedType(EValueType(serialized.cached_type()));
    }

    if (serialized.has_cached_name()) {
        result->SetCachedName(serialized.cached_name());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
