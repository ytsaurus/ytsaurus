#include "folding_profiler.h"
#include "cg_fragment_compiler.h"
#include "functions.h"
#include "functions_cg.h"
#include "plan_helpers.h"

namespace NYT {
namespace NQueryClient {

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

class TSchemaProfiler
    : private TNonCopyable
{
public:
    TSchemaProfiler(llvm::FoldingSetNodeID* id)
        : Id_(id)
    { }

    void Profile(const TTableSchema& tableSchema, int keySize = std::numeric_limits<int>::max());

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

void TSchemaProfiler::Profile(const TTableSchema& tableSchema, int keySize)
{
    Fold(static_cast<int>(EFoldingObjectType::TableSchema));
    Fold(keySize);
    for (int index = 0; index < tableSchema.Columns().size() && index < keySize; ++index) {
        const auto& column = tableSchema.Columns()[index];
        Fold(static_cast<ui16>(column.Type));
        Fold(column.Name.c_str());
        if (column.Expression) {
            Fold(column.Expression.Get().c_str());
        }
    }

    int aggregateColumnCount = 0;
    for (int index = keySize; index < tableSchema.Columns().size(); ++index) {
        if(tableSchema.Columns()[index].Aggregate) {
            ++aggregateColumnCount;
        }
    }
    Fold(aggregateColumnCount);
    for (int index = keySize; index < tableSchema.Columns().size(); ++index) {
        const auto& column = tableSchema.Columns()[index];
        Fold(index);
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
        const TConstFunctionProfilerMapPtr& functionProfilers)
        : TSchemaProfiler(id)
        , FunctionProfilers_(functionProfilers)
    { }

    TCodegenExpression Profile(TConstExpressionPtr expr, const TTableSchema& tableSchema);

    void Set(TCGVariables* variables);
    void Set(std::vector<std::vector<bool>>* literalArgs);

protected:
    TCGVariables* Variables_ = nullptr;
    std::vector<std::vector<bool>>* LiteralArgs_ = nullptr;

    TConstFunctionProfilerMapPtr FunctionProfilers_;
};

void TExpressionProfiler::Set(TCGVariables* variables)
{
    Variables_ = variables;
}

void TExpressionProfiler::Set(std::vector<std::vector<bool>>* literalArgs)
{
    LiteralArgs_ = literalArgs;
}

TCodegenExpression TExpressionProfiler::Profile(TConstExpressionPtr expr, const TTableSchema& schema)
{
    Fold(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::LiteralExpr));
        Fold(static_cast<ui16>(TValue(literalExpr->Value).Type));

        int index = Variables_
            ? Variables_->ConstantsRowBuilder.AddValue(TValue(literalExpr->Value))
            : -1;

        return MakeCodegenLiteralExpr(index, literalExpr->Type);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        Fold(referenceExpr->ColumnName.c_str());

        return MakeCodegenReferenceExpr(
            schema.GetColumnIndexOrThrow(referenceExpr->ColumnName),
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

        int index = -1;
        if (LiteralArgs_) {
            index =  LiteralArgs_->size();
            LiteralArgs_->push_back(std::move(literalArgs));
        }

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

        int index = -1;
        if (Variables_) {
            index = Variables_->LiteralRows.size();
            Variables_->LiteralRows.push_back(inOp->Values);
        }

        return MakeCodegenInOpExpr(codegenArgs, index);
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

class TQueryProfiler
    : public TExpressionProfiler
{
public:
    TQueryProfiler(
        llvm::FoldingSetNodeID* id,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers)
        : TExpressionProfiler(id, functionProfilers)
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
    TSchemaProfiler::Profile(query->RenamedTableSchema);
    TCodegenSource codegenSource = &CodegenScanOp;

    TTableSchema schema = query->RenamedTableSchema;

    for (const auto& joinClause : query->JoinClauses) {
        Fold(static_cast<int>(EFoldingObjectType::JoinOp));

        TSchemaProfiler::Profile(schema);
        TSchemaProfiler::Profile(joinClause->RenamedTableSchema);

        std::vector<TCodegenExpression> selfKeys;
        for (const auto& column : joinClause->Equations) {
            selfKeys.push_back(TExpressionProfiler::Profile(column.first, schema));
            TExpressionProfiler::Profile(column.second, joinClause->RenamedTableSchema);
        }

        if (auto selfFilter = ExtractPredicateForColumnSubset(query->WhereClause, schema)) {
            codegenSource = MakeCodegenFilterOp(TExpressionProfiler::Profile(selfFilter, schema), std::move(codegenSource));
        }

        std::vector<TCodegenExpression> evaluatedColumns;
        for (const auto& column : joinClause->EvaluatedColumns) {
            if (column) {
                evaluatedColumns.push_back(TExpressionProfiler::Profile(column, joinClause->ForeignTableSchema));
            } else {
                evaluatedColumns.emplace_back();
            }
        }

        codegenSource = MakeCodegenJoinOp(
            Variables_->JoinEvaluators.size(),
            selfKeys,
            schema,
            std::move(codegenSource),
            joinClause->KeyPrefix,
            joinClause->EquationByIndex,
            evaluatedColumns);

        Variables_->JoinEvaluators.push_back(GetJoinEvaluator(
            *joinClause,
            query->WhereClause,
            schema));

        schema = joinClause->JoinedTableSchema;
    }

    if (query->WhereClause) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        codegenSource = MakeCodegenFilterOp(
            TExpressionProfiler::Profile(query->WhereClause, schema),
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
            codegenAggregateExprs,
            codegenAggregates,
            groupClause->IsMerge,
            schema);

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
                keySize + codegenAggregates.size(),
                true);
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
                keySize + codegenAggregates.size(),
                true);
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
            schema,
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
        schema = query->ProjectClause->GetTableSchema();
    }

    return codegenSource;
}

TCodegenExpression TQueryProfiler::Profile(const TNamedItem& namedExpression, const TTableSchema& schema)
{
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));
    Fold(namedExpression.Name.c_str());

    return TExpressionProfiler::Profile(namedExpression.Expression, schema);
}

////////////////////////////////////////////////////////////////////////////////

void Profile(
    const TTableSchema& tableSchema,
    int keySize,
    llvm::FoldingSetNodeID* id)
{
    TSchemaProfiler profiler(id);
    profiler.Profile(tableSchema, keySize);
}

TCGExpressionCallbackGenerator Profile(
    TConstExpressionPtr expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    std::vector<std::vector<bool>>* literalArgs,
    const TConstFunctionProfilerMapPtr& functionProfilers)
{
    TExpressionProfiler profiler(id, functionProfilers);
    profiler.Set(variables);
    profiler.Set(literalArgs);

    return [
            codegenExpr = profiler.Profile(expr, schema)
        ] () {
            return CodegenExpression(std::move(codegenExpr));
        };
}

TCGQueryCallbackGenerator Profile(
    TConstQueryPtr query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    std::vector<std::vector<bool>>* literalArgs,
    const TConstFunctionProfilerMapPtr& functionProfilers,
    const TConstAggregateProfilerMapPtr& aggregateProfilers)
{
    TQueryProfiler profiler(id, functionProfilers, aggregateProfilers);
    profiler.Set(variables);
    profiler.Set(literalArgs);

    return [
            codegenSource = profiler.Profile(query)
        ] () {
            return CodegenEvaluate(std::move(codegenSource));
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

