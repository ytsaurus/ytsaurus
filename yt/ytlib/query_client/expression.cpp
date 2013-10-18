#include "ast.h"
#include "ast_visitor.h"
#include "query_context.h"

#include <yt/ytlib/query_client/expression.pb.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;
using NYT::FromProto;

TExpression::TExpression(TQueryContext* context, const TSourceLocation& sourceLocation)
    : TTrackedObject(context)
    , SourceLocation_(sourceLocation)
    , Parent_(nullptr)
    , Children_()
{ }

TExpression::~TExpression()
{ }

const SmallVectorImpl<TExpression*>& TExpression::Children() const
{
    return Children_;
}

TExpression* const* TExpression::ChildBegin() const
{
    return Children_.begin();
}

TExpression* const* TExpression::ChildEnd() const
{
    return Children_.end();
}

TExpression* TExpression::Parent() const
{
    return Parent_;
}

void TExpression::AttachChild(TExpression* expr)
{
    YCHECK(!expr->Parent_);
    Children_.push_back(expr);
    expr->Parent_ = this;
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

const char* TExpression::GetDebugName() const
{
    return typeid(*this).name();
}

void TExpression::Check() const
{
    FOREACH (const auto& child, Children_) {
        YCHECK(this == child->Parent_);
    }
}

#define XX(nodeType) IMPLEMENT_AST_VISITOR_HOOK(nodeType)
#include "list_of_expressions.inc"
#undef XX

////////////////////////////////////////////////////////////////////////////////

namespace {
class TToProtoVisitor
    : public IExpressionAstVisitor
{
public:
    TToProtoVisitor(NProto::TExpression* baseProto)
        : BaseProto_(baseProto)
    { }

    virtual bool Visit(TIntegerLiteralExpression* expr) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(
            NProto::TIntegerLiteralExpression::integer_literal_expression);
        derivedProto->set_value(expr->GetValue());
        return true;
    }

    virtual bool Visit(TDoubleLiteralExpression* expr) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(
            NProto::TIntegerLiteralExpression::integer_literal_expression);
        derivedProto->set_value(expr->GetValue());
        return true;
    }

    virtual bool Visit(TReferenceExpression* expr) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(
            NProto::TReferenceExpression::reference_expression);
        derivedProto->set_table_index(expr->GetTableIndex());
        derivedProto->set_name(Stroka(expr->GetName()));
        derivedProto->set_type(expr->GetType());
        return true;
    }

    virtual bool Visit(TFunctionExpression* expr) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(
            NProto::TFunctionExpression::function_expression);
        (void)derivedProto;
        YUNIMPLEMENTED();
        return true;
    }

    virtual bool Visit(TBinaryOpExpression* expr) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(
            NProto::TBinaryOpExpression::binary_op_expression);
        derivedProto->set_opcode(expr->GetOpcode());
        return true;
    }

private:
    NProto::TExpression* BaseProto_;

};
}

void ToProto(NProto::TExpression* serialized, TExpression* original)
{
    TToProtoVisitor visitor(serialized);
    original->Accept(&visitor);
    ToProto(serialized->mutable_children(), original->Children_);
}

TExpression* FromProto(const NProto::TExpression& serialized, TQueryContext* context)
{
    TExpression* result = nullptr;

    if (serialized.HasExtension(NProto::TIntegerLiteralExpression::integer_literal_expression)) {
        auto data = serialized.GetExtension(NProto::TIntegerLiteralExpression::integer_literal_expression);
        auto typedResult = new (context) TIntegerLiteralExpression(
            context,
            NullSourceLocation,
            data.value());
        result = typedResult;
    }

    if (serialized.HasExtension(NProto::TDoubleLiteralExpression::double_literal_expression)) {
        auto data = serialized.GetExtension(NProto::TDoubleLiteralExpression::double_literal_expression);
        auto typedResult = new (context) TDoubleLiteralExpression(
            context,
            NullSourceLocation,
            data.value());
        result = typedResult;
    }

    if (serialized.HasExtension(NProto::TReferenceExpression::reference_expression)) {
        auto data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
        auto typedResult = new (context) TReferenceExpression(
            context,
            NullSourceLocation,
            data.table_index(),
            data.name());
        typedResult->SetType(EColumnType(data.type()));
        result = typedResult;
    }

    if (serialized.HasExtension(NProto::TFunctionExpression::function_expression)) {
        auto data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
        YUNIMPLEMENTED();
    }

    if (serialized.HasExtension(NProto::TBinaryOpExpression::binary_op_expression)) {
        auto data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
        auto typedResult = new (context) TBinaryOpExpression(
            context,
            NullSourceLocation,
            EBinaryOp(data.opcode()));
        result = typedResult;
    }

    for (const auto& serializedChild : serialized.children()) {
        result->AttachChild(FromProto(serializedChild, context));
    }


    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

