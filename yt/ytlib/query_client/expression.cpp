#include "ast.h"
#include "ast_visitor.h"
#include "query_context.h"

#include <yt/ytlib/query_client/expression.pb.h>

#include <core/misc/protobuf_helpers.h>

#ifdef __GNUC__ 
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch-enum"
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;
using NYT::FromProto;

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
            proto->set_table_index(expr->GetTableIndex());
            proto->set_name(expr->GetName());
            proto->set_cached_type(expr->GetCachedType());
            proto->set_cached_key_index(expr->GetCachedKeyIndex());
            break;
        }

        case EExpressionKind::Function: {
            auto* expr = original->As<TFunctionExpression>();
            auto* proto = serialized->MutableExtension(NProto::TFunctionExpression::function_expression);
            proto->set_name(expr->GetName());
            ToProto(proto->mutable_arguments(), expr->Arguments());
            break;
        }

        case EExpressionKind::BinaryOp: {
            auto* expr = original->As<TBinaryOpExpression>();
            auto* proto = serialized->MutableExtension(NProto::TBinaryOpExpression::binary_op_expression);
            ToProto(proto->mutable_lhs(), expr->GetLhs());
            ToProto(proto->mutable_rhs(), expr->GetRhs());
            break;
        }

        default:
            YUNREACHABLE();
    }

}

const TExpression* FromProto(const NProto::TExpression& serialized, TQueryContext* context)
{
    const TExpression* result = nullptr;

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
                data.name());
            typedResult->SetCachedType(EColumnType(data.cached_type()));
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
                data.name());
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

        default:
            YUNREACHABLE();
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
