#include "ast_helpers.h"

#include "private.h"
#include "helpers.h"

#include "ast.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const TDataSplit& GetHeaviestSplit(const TOperator* op)
{
    switch (op->GetKind()) {
    case EOperatorKind::Scan:
            return op->As<TScanOperator>()->DataSplit();
        case EOperatorKind::Filter:
            return GetHeaviestSplit(op->As<TFilterOperator>()->GetSource());
        case EOperatorKind::Project:
            return GetHeaviestSplit(op->As<TProjectOperator>()->GetSource());
        default:
            YUNREACHABLE();
    }
}

TTableSchema InferTableSchema(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan:
            return GetTableSchemaFromDataSplit(op->As<TScanOperator>()->DataSplit());
        case EOperatorKind::Filter:
            return InferTableSchema(op->As<TFilterOperator>()->GetSource());
        case EOperatorKind::Project: {
            TTableSchema result;
            auto* typedOp = op->As<TProjectOperator>();
            for (const auto& projection : typedOp->Projections()) {
                result.Columns().emplace_back(
                    projection->InferName(),
                    projection->Typecheck());
            }
            return result;
        }
        case EOperatorKind::Union: {
            TTableSchema result;
            bool didChooseTableSchema = false;
            auto* typedOp = op->As<TUnionOperator>();
            for (const auto& source : typedOp->Sources()) {
                if (!didChooseTableSchema) {
                    result = InferTableSchema(source);
                    didChooseTableSchema = true;
                } else {
                    YCHECK(result == InferTableSchema(source));
                }
            }
            return result;
        }
        default:
            YUNREACHABLE();
    }
}

TKeyColumns InferKeyColumns(const TOperator* op)
{
    switch (op->GetKind()) {
        case EOperatorKind::Scan:
            return GetKeyColumnsFromDataSplit(op->As<TScanOperator>()->DataSplit());
        case EOperatorKind::Filter:
            return InferKeyColumns(op->As<TFilterOperator>()->GetSource());
        case EOperatorKind::Project:
            return TKeyColumns();
        case EOperatorKind::Union: {
            TKeyColumns result;
            bool didChooseKeyColumns = false;
            auto* typedOp = op->As<TUnionOperator>();
            for (const auto& source : typedOp->Sources()) {
                if (!didChooseKeyColumns) {
                    result = InferKeyColumns(source);
                    didChooseKeyColumns = true;
                } else {
                    YCHECK(result == InferKeyColumns(source));
                }
            }
            return result;
        }
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

