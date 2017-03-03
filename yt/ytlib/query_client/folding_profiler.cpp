#include "folding_profiler.h"
#include "cg_fragment_compiler.h"
#include "functions.h"
#include "functions_cg.h"
#include "query_helpers.h"

namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFoldingObjectType,
    (ScanOp)
    (JoinOp)
    (FilterOp)
    (GroupOp)
    (HavingOp)
    (OrderOp)
    (ProjectOp)

    (LiteralExpr)
    (ReferenceExpr)
    (FunctionExpr)
    (UnaryOpExpr)
    (BinaryOpExpr)
    (InOpExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
);

//! Computes a strong structural hash used to cache query fragments.

////////////////////////////////////////////////////////////////////////////////

std::vector<EValueType> GetTypesFromSchema(const TTableSchema& tableSchema)
{
    std::vector<EValueType> result;

    for (const auto& column : tableSchema.Columns()) {
        result.push_back(column.Type);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TSchemaProfiler
    : private TNonCopyable
{
public:
    TSchemaProfiler(llvm::FoldingSetNodeID* id)
        : Id_(id)
    { }

    void Profile(const TTableSchema& tableSchema);

protected:
    void Fold(int numeric);
    void Fold(const char* str);

    llvm::FoldingSetNodeID* Id_;

};

void TSchemaProfiler::Fold(int numeric)
{
    if (Id_) {
        Id_->AddInteger(numeric);
    }
}

void TSchemaProfiler::Fold(const char* str)
{
    if (Id_) {
        Id_->AddString(str);
    }
}

void TSchemaProfiler::Profile(const TTableSchema& tableSchema)
{
    const auto& columns = tableSchema.Columns();
    Fold(static_cast<int>(EFoldingObjectType::TableSchema));
    for (int index = 0; index < columns.size(); ++index) {
        const auto& column = columns[index];
        Fold(static_cast<ui16>(column.Type));
        Fold(column.Name.c_str());
        int aux = (column.Expression ? 1 : 0) | ((column.Aggregate ? 1 : 0) << 1);
        Fold(aux);
        if (column.Expression) {
            Fold(column.Expression.Get().c_str());
        }
        if (column.Aggregate) {
            Fold(column.Aggregate.Get().c_str());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TExpressionProfiler
    : public TSchemaProfiler
{
public:
    TExpressionProfiler(
        llvm::FoldingSetNodeID* id,
        TCGVariables* variables,
        const TConstFunctionProfilerMapPtr& functionProfilers)
        : TSchemaProfiler(id)
        , Variables_(variables)
        , FunctionProfilers_(functionProfilers)
    {
        YCHECK(variables);
    }

    TCodegenExpression Profile(TConstExpressionPtr expr, const TTableSchema& schema);

protected:
    TCGVariables* Variables_;

    TConstFunctionProfilerMapPtr FunctionProfilers_;
};

TCodegenExpression TExpressionProfiler::Profile(TConstExpressionPtr expr, const TTableSchema& schema)
{
    Fold(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::LiteralExpr));
        Fold(static_cast<ui16>(TValue(literalExpr->Value).Type));

        int index = Variables_->AddOpaque<TOwningValue>(literalExpr->Value);

        return MakeCodegenLiteralExpr(index, literalExpr->Type);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        auto indexInSchema = schema.GetColumnIndexOrThrow(referenceExpr->ColumnName);
        Fold(indexInSchema);

        return MakeCodegenReferenceExpr(
            indexInSchema,
            referenceExpr->Type,
            referenceExpr->ColumnName);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::FunctionExpr));
        Fold(functionExpr->FunctionName.c_str());

        std::vector<TCodegenExpression> codegenArgs;
        std::vector<EValueType> argumentTypes;
        std::vector<bool> literalArgs;
        for (const auto& argument : functionExpr->Arguments) {
            codegenArgs.push_back(Profile(argument, schema));
            argumentTypes.push_back(argument->Type);
            literalArgs.push_back(argument->As<TLiteralExpression>() != nullptr);
        }

        int index = Variables_->AddOpaque<TFunctionContext>(std::move(literalArgs));

        const auto& function = FunctionProfilers_->GetFunction(functionExpr->FunctionName);

        return function->Profile(
            MakeCodegenFunctionContext(index),
            std::move(codegenArgs),
            std::move(argumentTypes),
            functionExpr->Type,
            "{" + InferName(functionExpr, true) + "}",
            Id_);
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::UnaryOpExpr));
        Fold(static_cast<int>(unaryOp->Opcode));

        return MakeCodegenUnaryOpExpr(
            unaryOp->Opcode,
            Profile(unaryOp->Operand, schema),
            unaryOp->Type,
            "{" + InferName(unaryOp, true) + "}");
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        Fold(static_cast<int>(binaryOp->Opcode));

        return MakeCodegenBinaryOpExpr(
            binaryOp->Opcode,
            Profile(binaryOp->Lhs, schema),
            Profile(binaryOp->Rhs, schema),
            binaryOp->Type,
            "{" + InferName(binaryOp, true) + "}");
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::InOpExpr));

        std::vector<TCodegenExpression> codegenArgs;
        for (const auto& argument : inOp->Arguments) {
            codegenArgs.push_back(Profile(argument, schema));
        }

        int index = Variables_->AddOpaque<TSharedRange<TRow>>(inOp->Values);

        return MakeCodegenInOpExpr(codegenArgs, index);
    }

    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

class TQueryProfiler
    : public TExpressionProfiler
{
public:
    TQueryProfiler(
        llvm::FoldingSetNodeID* id,
        TCGVariables* variables,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers)
        : TExpressionProfiler(id, variables, functionProfilers)
        , AggregateProfilers_(aggregateProfilers)
    { }

    TCodegenSource Profile(TConstQueryPtr query);

protected:
    TCodegenExpression Profile(const TNamedItem& namedExpression, const TTableSchema& schema);

    TConstAggregateProfilerMapPtr AggregateProfilers_;
};

TCodegenSource TQueryProfiler::Profile(TConstQueryPtr query)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));
    TCodegenSource codegenSource = &CodegenScanOp;

    auto schema = query->GetRenamedSchema();
    auto whereClause = query->WhereClause;

    TSchemaProfiler::Profile(schema);

    for (const auto& joinClause : query->JoinClauses) {
        Fold(static_cast<int>(EFoldingObjectType::JoinOp));

        std::vector<std::pair<TCodegenExpression, bool>> selfKeys;

        for (const auto& column : joinClause->SelfEquations) {
            TConstExpressionPtr expression;
            bool isEvaluated;
            std::tie(expression, isEvaluated) = column;

            const auto& expressionSchema = isEvaluated ? joinClause->OriginalSchema : schema;
            selfKeys.emplace_back(TExpressionProfiler::Profile(expression, expressionSchema), isEvaluated);
        }

        TConstExpressionPtr selfFilter;
        std::tie(selfFilter, whereClause) = SplitPredicateByColumnSubset(whereClause, schema);

        if (selfFilter) {
            codegenSource = MakeCodegenFilterOp(
                TExpressionProfiler::Profile(selfFilter, schema),
                std::move(codegenSource));
        }

        size_t joinBatchSize = std::numeric_limits<size_t>::max();

        if (query->IsOrdered()) {
            joinBatchSize = query->Limit;
        }

        TConstExpressionPtr foreignFilter;
        if (!joinClause->IsLeft) {
            std::tie(foreignFilter, whereClause) = SplitPredicateByColumnSubset(
                whereClause,
                joinClause->GetRenamedSchema());
        }

        int index = Variables_->AddOpaque<TJoinParameters>(GetJoinEvaluator(
            *joinClause,
            foreignFilter,
            schema,
            query->InputRowLimit,
            query->OutputRowLimit,
            joinBatchSize,
            query->IsOrdered()));

        codegenSource = MakeCodegenJoinOp(
            index,
            selfKeys,
            std::move(codegenSource));

        schema = joinClause->GetTableSchema(schema);
        TSchemaProfiler::Profile(schema);
    }

    if (whereClause) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        codegenSource = MakeCodegenFilterOp(
            TExpressionProfiler::Profile(whereClause, schema),
            std::move(codegenSource));
    }

    if (auto groupClause = query->GroupClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::GroupOp));

        std::vector<TCodegenExpression> codegenGroupExprs;
        std::vector<TCodegenExpression> codegenAggregateExprs;
        std::vector<TCodegenAggregate> codegenAggregates;

        std::vector<EValueType> keyTypes;

        for (const auto& groupItem : groupClause->GroupItems) {
            codegenGroupExprs.push_back(Profile(groupItem, schema));
            keyTypes.push_back(groupItem.Expression->Type);
        }

        for (const auto& aggregateItem : groupClause->AggregateItems) {
            Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
            Fold(aggregateItem.AggregateFunction.c_str());
            Fold(aggregateItem.Name.c_str());

            const auto& aggregate = AggregateProfilers_->GetAggregate(aggregateItem.AggregateFunction);

            codegenAggregateExprs.push_back(TExpressionProfiler::Profile(aggregateItem.Expression, schema));
            codegenAggregates.push_back(aggregate->Profile(
                aggregateItem.Expression->Type,
                aggregateItem.StateType,
                aggregateItem.ResultType,
                aggregateItem.Name,
                Id_));
        }

        size_t keySize = keyTypes.size();

        auto initialize = MakeCodegenAggregateInitialize(
            codegenAggregates,
            keySize);

        auto aggregate = MakeCodegenEvaluateAggregateArgs(
            keySize,
            codegenAggregateExprs);

        auto update = MakeCodegenAggregateUpdate(
            codegenAggregates,
            keySize,
            groupClause->IsMerge);

        auto finalize = MakeCodegenAggregateFinalize(
            codegenAggregates,
            keySize,
            groupClause->IsFinal);

        codegenSource = MakeCodegenGroupOp(
            initialize,
            MakeCodegenEvaluateGroups(codegenGroupExprs),
            aggregate,
            update,
            finalize,
            std::move(codegenSource),
            keyTypes,
            groupClause->IsMerge,
            keySize + codegenAggregates.size(),
            false,
            groupClause->TotalsMode != ETotalsMode::None);

        schema = groupClause->GetTableSchema();

        if (groupClause->TotalsMode == ETotalsMode::BeforeHaving) {
            codegenSource = MakeCodegenGroupOp(
                initialize,
                MakeCodegenEvaluateGroups( // Codegen nulls here
                    std::vector<TCodegenExpression>(),
                    keyTypes),
                aggregate,
                update,
                finalize,
                std::move(codegenSource),
                keyTypes,
                groupClause->IsMerge,
                keySize + codegenAggregates.size(),
                true,
                false);
        }

        if (query->HavingClause) {
            Fold(static_cast<int>(EFoldingObjectType::HavingOp));
            codegenSource = MakeCodegenFilterOp(
                TExpressionProfiler::Profile(query->HavingClause, schema),
                std::move(codegenSource));
        }

        if (groupClause->TotalsMode == ETotalsMode::AfterHaving) {
            codegenSource = MakeCodegenGroupOp(
                initialize,
                MakeCodegenEvaluateGroups( // Codegen nulls here
                    std::vector<TCodegenExpression>(),
                    keyTypes),
                aggregate,
                update,
                finalize,
                std::move(codegenSource),
                keyTypes,
                groupClause->IsMerge,
                keySize + codegenAggregates.size(),
                true,
                false);
        }
    }

    if (auto orderClause = query->OrderClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::OrderOp));

        std::vector<TCodegenExpression> codegenOrderExprs;
        std::vector<bool> isDesc;

        for (const auto& item : orderClause->OrderItems) {
            codegenOrderExprs.push_back(TExpressionProfiler::Profile(item.first, schema));
            Fold(item.second);
            isDesc.push_back(item.second);
        }

        codegenSource = MakeCodegenOrderOp(
            codegenOrderExprs,
            GetTypesFromSchema(schema),
            std::move(codegenSource),
            isDesc);
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        std::vector<TCodegenExpression> codegenProjectExprs;

        for (const auto& item : projectClause->Projections) {
            codegenProjectExprs.push_back(Profile(item, schema));
        }

        codegenSource = MakeCodegenProjectOp(std::move(codegenProjectExprs), std::move(codegenSource));
        schema = projectClause->GetTableSchema();
    }

    return codegenSource;
}

TCodegenExpression TQueryProfiler::Profile(const TNamedItem& namedExpression, const TTableSchema& schema)
{
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));
    Fold(namedExpression.Name.c_str());

    return TExpressionProfiler::Profile(namedExpression.Expression, schema);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Profile(
    const TTableSchema& tableSchema,
    llvm::FoldingSetNodeID* id)
{
    TSchemaProfiler profiler(id);
    profiler.Profile(tableSchema);
}

TCGExpressionCallbackGenerator Profile(
    TConstExpressionPtr expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers)
{
    TExpressionProfiler profiler(id, variables, functionProfilers);

    auto codegenExpr = profiler.Profile(expr, schema);

    return [
            MOVE(codegenExpr),
            opaqueValuesCount = variables->GetOpaqueCount()
        ] () {
            return CodegenExpression(std::move(codegenExpr), opaqueValuesCount);
        };
}

TCGQueryCallbackGenerator Profile(
    TConstQueryPtr query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers,
    const TConstAggregateProfilerMapPtr& aggregateProfilers)
{
    TQueryProfiler profiler(id, variables, functionProfilers, aggregateProfilers);

    auto codegenSource = profiler.Profile(query);

    return [
            MOVE(codegenSource),
            opaqueValuesCount = variables->GetOpaqueCount()
        ] () {
            return CodegenEvaluate(std::move(codegenSource), opaqueValuesCount);
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

