#include "stdafx.h"
#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_context.h"

#include <yt/ytlib/query_client/expression.pb.h>

#include <yt/ytlib/new_table_client/unversioned_row.h>

#include <core/misc/protobuf_helpers.h>

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
    }
    YUNREACHABLE();
}

TExpression* TExpression::CloneImpl(TPlanContext* context) const
{
    TExpression* result = nullptr;

    switch (GetKind()) {
#define XX(kind) \
        case EExpressionKind::kind: \
            result = new (context) T##kind##Expression(context, *this->As<T##kind##Expression>()); \
            break;
#include "list_of_expressions.inc"
#undef XX
    }

    YCHECK(result);
    return result;
}

Stroka TExpression::GetSource() const
{
    auto source = Context_->GetSource();

    if (!source.empty()) {
        auto begin = SourceLocation_.first;
        auto end = SourceLocation_.second;

        source = source.substr(begin, end - begin);
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

        case EExpressionKind::Literal: {
            auto* expr = original->As<TLiteralExpression>();
            auto* proto = serialized->MutableExtension(NProto::TLiteralExpression::literal_expression);

            auto value = expr->GetValue();
            auto data = value.Data;

            proto->set_type(value.Type);

            switch (value.Type) {

                case EValueType::Int64: {
                    proto->set_int64_value(data.Int64);
                    break;
                }

                case EValueType::Uint64: {
                    proto->set_uint64_value(data.Uint64);
                    break;
                }
                
                case EValueType::Double: {
                    proto->set_double_value(data.Double);
                    break;
                }

                case EValueType::String: {
                    proto->set_string_value(data.String, value.Length);
                    break;
                }

                case EValueType::Boolean: {
                    proto->set_boolean_value(data.Boolean);
                    break;
                }

                default:
                    YUNREACHABLE();
            }
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

    }
}

const TExpression* FromProto(const NProto::TExpression& serialized, TPlanContext* context)
{
    using namespace NVersionedTableClient;
    TExpression* result = nullptr;

    switch (EExpressionKind(serialized.kind())) {

        case EExpressionKind::Literal: {
            auto data = serialized.GetExtension(NProto::TLiteralExpression::literal_expression);
            TLiteralExpression* typedResult = nullptr;
            EValueType type(data.type());

            switch (type) {

                case EValueType::Int64: {
                    typedResult = new (context) TLiteralExpression(
                        context,
                        NullSourceLocation,
                        MakeUnversionedInt64Value(data.int64_value()));
                    break;
                }

                case EValueType::Uint64: {
                    typedResult = new (context) TLiteralExpression(
                        context,
                        NullSourceLocation,
                        MakeUnversionedUint64Value(data.uint64_value()));
                    break;
                }

                case EValueType::Double: {
                    typedResult = new (context) TLiteralExpression(
                        context,
                        NullSourceLocation,
                        MakeUnversionedDoubleValue(data.double_value()));
                    break;
                }

                case EValueType::String: {
                    typedResult = new (context) TLiteralExpression(
                        context,
                        NullSourceLocation,
                        MakeUnversionedStringValue(context->Capture(data.string_value())));
                    break;
                }

                case EValueType::Boolean: {
                    typedResult = new (context) TLiteralExpression(
                        context,
                        NullSourceLocation,
                        MakeUnversionedBooleanValue(data.boolean_value()));
                    break;
                }

                default:
                    YUNREACHABLE();
            }

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

        default:
            YUNREACHABLE();
    }

    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

