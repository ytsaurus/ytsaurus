#include "ast.h"
#include "ast_visitor.h"
#include "query_context.h"

#include <yt/ytlib/query_client/operator.pb.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;
using NYT::FromProto;

#define XX(nodeType) IMPLEMENT_AST_VISITOR_HOOK(nodeType)
#include "list_of_operators.inc"
#undef XX

////////////////////////////////////////////////////////////////////////////////

namespace {
class TToProtoVisitor
    : public IOperatorAstVisitor
{
public:
    TToProtoVisitor(NProto::TOperator* baseProto)
        : BaseProto_(baseProto)
    { }

    virtual bool Visit(TScanOperator* op) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(NProto::TScanOperator::scan_operator);
        derivedProto->set_table_index(op->GetTableIndex());
        ToProto(derivedProto->mutable_data_split(), op->DataSplit());
        return true;
    }

    virtual bool Visit(TFilterOperator* op) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(NProto::TFilterOperator::filter_operator);
        ToProto(derivedProto->mutable_predicate(), op->GetPredicate());
        return true;
    }

    virtual bool Visit(TProjectOperator* op) override
    {
        auto* derivedProto = BaseProto_->MutableExtension(NProto::TProjectOperator::project_operator);
        ToProto(derivedProto->mutable_expressions(), op->Expressions());
        return true;
    }

private:
    NProto::TOperator* BaseProto_;

};
}

void ToProto(NProto::TOperator* serialized, TOperator* original)
{
    TToProtoVisitor visitor(serialized);
    original->Accept(&visitor);
    ToProto(serialized->mutable_children(), original->Children());
}

TOperator* FromProto(const NProto::TOperator& serialized, TQueryContext* context)
{
    TOperator* result = nullptr;

    if (serialized.HasExtension(NProto::TScanOperator::scan_operator)) {
        auto data = serialized.GetExtension(NProto::TScanOperator::scan_operator);
        auto typedResult = new (context) TScanOperator(
            context,
            data.table_index());
        FromProto(&typedResult->DataSplit(), data.data_split());
        result = typedResult;
    }

    if (serialized.HasExtension(NProto::TFilterOperator::filter_operator)) {
        auto data = serialized.GetExtension(NProto::TFilterOperator::filter_operator);
        auto typedResult = new (context) TFilterOperator(
            context,
            FromProto(data.predicate(), context));
        result = typedResult;
    }

    if (serialized.HasExtension(NProto::TProjectOperator::project_operator)) {
        auto data = serialized.GetExtension(NProto::TProjectOperator::project_operator);
        auto typedResult = new (context) TProjectOperator(context);

        typedResult->Expressions().reserve(data.expressions_size());
        for (int i = 0; i < data.expressions_size(); ++i) {
            typedResult->Expressions().push_back(
                FromProto(data.expressions(i), context));
        }
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

