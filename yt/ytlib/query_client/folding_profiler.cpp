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
    (SplitterOp)
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
    (IsFinal)
    (IsMerge)
    (UseDisjointGroupBy)
    (TotalsMode)
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
    void Fold(size_t numeric);
    void Fold(const char* str);

    llvm::FoldingSetNodeID* Id_;

};

void TSchemaProfiler::Fold(int numeric)
{
    if (Id_) {
        Id_->AddInteger(numeric);
    }
}

void TSchemaProfiler::Fold(size_t numeric)
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

    void Profile(
        TCodegenSource* codegenSource,
        TConstBaseQueryPtr query,
        size_t* slotCount,
        size_t finalSlot,
        size_t intermediateSlot,
        size_t totalsSlot,
        TTableSchema schema,
        bool isMerge);

    void Profile(TCodegenSource* codegenSource, TConstQueryPtr query, size_t* slotCount);

    void Profile(TCodegenSource* codegenSource, TConstFrontQueryPtr query, size_t* slotCount);

protected:
    TCodegenExpression Profile(const TNamedItem& namedExpression, const TTableSchema& schema);

    TConstAggregateProfilerMapPtr AggregateProfilers_;
};

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    TConstBaseQueryPtr query,
    size_t* slotCount,
    size_t finalSlot,
    size_t intermediateSlot,
    size_t totalsSlot,
    TTableSchema schema,
    bool isMerge)
{
    size_t dummySlot = (*slotCount)++;

    bool isFinal = query->IsFinal;
    bool isIntermediate = isMerge && !isFinal;
    bool useDisjointGroupBy = query->UseDisjointGroupBy;

    Fold(static_cast<int>(EFoldingObjectType::IsFinal));
    Fold(static_cast<int>(isFinal));
    Fold(static_cast<int>(EFoldingObjectType::IsMerge));
    Fold(static_cast<int>(isMerge));
    Fold(static_cast<int>(EFoldingObjectType::UseDisjointGroupBy));
    Fold(static_cast<int>(useDisjointGroupBy));

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

            if (!isMerge) {
                codegenAggregateExprs.push_back(TExpressionProfiler::Profile(aggregateItem.Expression, schema));
            }

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
            isMerge);

        auto finalize = MakeCodegenAggregateFinalize(
            codegenAggregates,
            keySize);

        intermediateSlot = MakeCodegenGroupOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            initialize,
            MakeCodegenEvaluateGroups(codegenGroupExprs),
            aggregate,
            update,
            keyTypes,
            isMerge,
            keySize + codegenAggregates.size(),
            groupClause->TotalsMode != ETotalsMode::None);

        Fold(static_cast<int>(EFoldingObjectType::TotalsMode));
        Fold(static_cast<int>(groupClause->TotalsMode));

        schema = groupClause->GetTableSchema(query->IsFinal);

        if (useDisjointGroupBy && !isMerge || isFinal) {
            intermediateSlot = MakeCodegenFinalizeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                finalize);

            if (groupClause->TotalsMode == ETotalsMode::BeforeHaving && !isIntermediate) {
                size_t totalsSlotNew;
                std::tie(totalsSlotNew, intermediateSlot) = MakeCodegenSplitOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot);

                totalsSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    totalsSlot,
                    totalsSlotNew);
            }

            if (query->HavingClause) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                intermediateSlot = MakeCodegenFilterOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot,
                    TExpressionProfiler::Profile(query->HavingClause, schema));
            }

            if (groupClause->TotalsMode == ETotalsMode::AfterHaving && !isIntermediate) {
                size_t totalsSlotNew;
                std::tie(totalsSlotNew, intermediateSlot) = MakeCodegenSplitOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot);

                totalsSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    totalsSlot,
                    totalsSlotNew);
            }

            finalSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot,
                    finalSlot);
            intermediateSlot = dummySlot;
        }

        if (groupClause->TotalsMode != ETotalsMode::None) {
            totalsSlot = MakeCodegenGroupOp(
                codegenSource,
                slotCount,
                totalsSlot,
                initialize,
                MakeCodegenEvaluateGroups( // Codegen nulls here
                    std::vector<TCodegenExpression>(),
                    keyTypes),
                aggregate,
                update,
                keyTypes,
                true,
                keySize + codegenAggregates.size(),
                false);

            if (isFinal) {
                totalsSlot = MakeCodegenFinalizeOp(
                    codegenSource,
                    slotCount,
                    totalsSlot,
                    finalize);

                if (groupClause->TotalsMode == ETotalsMode::BeforeHaving && query->HavingClause) {
                    Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                    totalsSlot = MakeCodegenFilterOp(
                        codegenSource,
                        slotCount,
                        totalsSlot,
                        TExpressionProfiler::Profile(query->HavingClause, schema));
                }
            }
        }
    } else {
        finalSlot = MakeCodegenMergeOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            finalSlot);
        intermediateSlot = dummySlot;
    }

    if (auto orderClause = query->OrderClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::OrderOp));

        std::vector<TCodegenExpression> codegenOrderExprs;
        std::vector<bool> isDesc;
        std::vector<EValueType> orderColumnTypes;

        for (const auto& item : orderClause->OrderItems) {
            codegenOrderExprs.push_back(TExpressionProfiler::Profile(item.first, schema));
            Fold(item.second);
            isDesc.push_back(item.second);
            orderColumnTypes.push_back(item.first->Type);
        }

        finalSlot = MakeCodegenOrderOp(
            codegenSource,
            slotCount,
            finalSlot,
            std::move(codegenOrderExprs),
            std::move(orderColumnTypes),
            GetTypesFromSchema(schema),
            std::move(isDesc));
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        std::vector<TCodegenExpression> codegenProjectExprs;

        for (const auto& item : projectClause->Projections) {
            codegenProjectExprs.push_back(Profile(item, schema));
        }

        finalSlot = MakeCodegenProjectOp(codegenSource, slotCount, finalSlot, codegenProjectExprs);
        totalsSlot = MakeCodegenProjectOp(codegenSource, slotCount, totalsSlot, codegenProjectExprs);

        schema = projectClause->GetTableSchema();
    }

    if (!isFinal) {
        finalSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                finalSlot,
                GetTypesFromSchema(schema),
                EStreamTag::Final);

        totalsSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                totalsSlot,
                GetTypesFromSchema(schema),
                EStreamTag::Totals);

        intermediateSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                GetTypesFromSchema(schema),
                EStreamTag::Intermediate);
    }

    size_t resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, finalSlot, totalsSlot);
    resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, resultSlot, intermediateSlot);

    MakeCodegenWriteOp(codegenSource, resultSlot);
}

void TQueryProfiler::Profile(TCodegenSource* codegenSource, TConstQueryPtr query, size_t* slotCount)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();

    size_t currentSlot = MakeCodegenScanOp(codegenSource, slotCount);

    auto whereClause = query->WhereClause;

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
            currentSlot = MakeCodegenFilterOp(
                codegenSource,
                slotCount,
                currentSlot,
                TExpressionProfiler::Profile(selfFilter, schema));
        }

        size_t joinBatchSize = std::numeric_limits<size_t>::max();

        if (query->IsOrdered()) {
            joinBatchSize = query->Limit;
        }

        TConstExpressionPtr joinPredicate = joinClause->Predicate;
        if (!joinClause->IsLeft) {
            TConstExpressionPtr foreignPredicate;
            std::tie(foreignPredicate, whereClause) = SplitPredicateByColumnSubset(
                whereClause,
                joinClause->GetRenamedSchema());

            if (joinPredicate) {
                joinPredicate = MakeAndExpression(joinPredicate, foreignPredicate);
            } else {
                joinPredicate = foreignPredicate;
            }
        }

        int index = Variables_->AddOpaque<TJoinParameters>(GetJoinEvaluator(
            *joinClause,
            joinPredicate,
            schema,
            query->InputRowLimit,
            query->OutputRowLimit,
            joinBatchSize,
            query->IsOrdered()));

        YCHECK(joinClause->CommonKeyPrefix < 1000);

        Fold(joinClause->CommonKeyPrefix);

        currentSlot = MakeCodegenJoinOp(
            codegenSource,
            slotCount,
            currentSlot,
            index,
            selfKeys,
            joinClause->CommonKeyPrefix);

        schema = joinClause->GetTableSchema(schema);
    }

    if (whereClause) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        currentSlot = MakeCodegenFilterOp(
            codegenSource,
            slotCount,
            currentSlot,
            TExpressionProfiler::Profile(whereClause, schema));
    }

    size_t dummySlot = (*slotCount)++;
    Profile(codegenSource, query, slotCount, dummySlot, currentSlot, dummySlot, schema, false);
}

void TQueryProfiler::Profile(TCodegenSource* codegenSource, TConstFrontQueryPtr query, size_t* slotCount)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();

    size_t currentSlot = MakeCodegenScanOp(codegenSource, slotCount);

    size_t finalSlot;
    size_t intermediateSlot;
    size_t totalsSlot;

    Fold(static_cast<int>(EFoldingObjectType::SplitterOp));

    std::tie(finalSlot, intermediateSlot, totalsSlot) = MakeCodegenSplitterOp(
        codegenSource,
        slotCount,
        currentSlot,
        schema.Columns().size());

    Profile(codegenSource, query, slotCount, finalSlot, intermediateSlot, totalsSlot, schema, true);
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
    TConstBaseQueryPtr query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers,
    const TConstAggregateProfilerMapPtr& aggregateProfilers)
{
    TQueryProfiler profiler(id, variables, functionProfilers, aggregateProfilers);

    size_t slotCount = 0;
    TCodegenSource codegenSource = &CodegenEmptyOp;

    if (auto derivedQuery = dynamic_cast<const TQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount);
    } else if (auto derivedQuery = dynamic_cast<const TFrontQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount);
    } else {
        Y_UNREACHABLE();
    }

    return [
            MOVE(codegenSource),
            slotCount,
            opaqueValuesCount = variables->GetOpaqueCount()
        ] () {
            return CodegenEvaluate(&codegenSource, slotCount, opaqueValuesCount);
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

