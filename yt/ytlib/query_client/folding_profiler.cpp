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
    void Fold(const llvm::FoldingSetNodeID& id);

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

void TSchemaProfiler::Fold(const llvm::FoldingSetNodeID& id)
{
    if (Id_) {
        Id_->AddNodeID(id);
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

struct TDebugInfo
{
    TConstExpressionPtr Expr;
    std::vector<size_t> Args;

    TDebugInfo(const TConstExpressionPtr& expr, const std::vector<size_t>& args)
        : Expr(expr)
        , Args(args)
    { }

};

TString InferName(
    size_t id,
    const std::vector<TDebugInfo>& debugExpressions,
    const std::vector<TCodegenFragmentInfo>& expressions);

TString InferNameArg(
    size_t id,
    const std::vector<TDebugInfo>& debugExpressions,
    const std::vector<TCodegenFragmentInfo>& expressions)
{
    if (expressions[id].IsOutOfLine()) {
        return Format("$%v", id);
    }

    return InferName(id, debugExpressions, expressions);
}

TString InferName(
    size_t id,
    const std::vector<TDebugInfo>& debugExpressions,
    const std::vector<TCodegenFragmentInfo>& expressions)
{
    const auto& expr = debugExpressions[id].Expr;
    const auto& args = debugExpressions[id].Args;

    bool newTuple = true;
    auto comma = [&] {
        bool isNewTuple = newTuple;
        newTuple = false;
        return TString(isNewTuple ? "" : ", ");
    };
    auto canOmitParenthesis = [&] (size_t id) {
        const auto& expr = debugExpressions[id].Expr;
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return ToString(static_cast<TValue>(literalExpr->Value));
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        auto columnName = referenceExpr->ColumnName;
        if (columnName.size() > 40) {
            columnName.resize(40);
            columnName.Transform([] (size_t index, char c) {
                if (c == '(' || c == ')') {
                    return '_';
                }
                return c;
            });

            return Format("[%x%v]", FarmFingerprint(columnName.data(), columnName.size()), columnName);
        }

        return Format("[%v]", columnName);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        auto str = functionExpr->FunctionName + "(";
        for (const auto& argument : args) {
            str += comma() + InferNameArg(argument, debugExpressions, expressions);
        }
        return str + ")";
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        auto rhsName = InferNameArg(args[0], debugExpressions, expressions);
        if (!canOmitParenthesis(args[0])) {
            rhsName = "(" + rhsName + ")";
        }
        return TString() + GetUnaryOpcodeLexeme(unaryOp->Opcode) + " " + rhsName;
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        auto lhsName = InferNameArg(args[0], debugExpressions, expressions);
        if (!canOmitParenthesis(args[0])) {
            lhsName = "(" + lhsName + ")";
        }
        auto rhsName = InferNameArg(args[1], debugExpressions, expressions);
        if (!canOmitParenthesis(args[1])) {
            rhsName = "(" + rhsName + ")";
        }
        return
            lhsName +
            " " + GetBinaryOpcodeLexeme(binaryOp->Opcode) + " " +
            rhsName;
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        TString str;
        for (const auto& argument : args) {
            str += comma() + InferNameArg(argument, debugExpressions, expressions);
        }
        if (inOp->Arguments.size() > 1) {
            str = "(" + str + ")";
        }
        str += " IN (";
        newTuple = true;
        for (const auto& row : inOp->Values) {
            str += comma() + ToString(row);
        }
        return str + ")";
    } else {
        Y_UNREACHABLE();
    }
}

static bool IsDumpExprsEnabled()
{
    static bool result = (getenv("DUMP_EXPRS") != nullptr);
    return result;
}

struct TExpressionFragments
{
    std::vector<TCodegenFragmentInfo> Items;
    yhash<llvm::FoldingSetNodeID, size_t> Fingerprints;
    std::vector<TDebugInfo> DebugInfos;

    TCodegenFragmentInfosPtr ToFragmentInfos(const TString& namePrefix)
    {
        if (IsDumpExprsEnabled()) {
            Cerr << "\n" << namePrefix << "\n";
        }

        auto result = New<TCodegenFragmentInfos>();
        result->Items.assign(Items.begin(), Items.end());
        result->NamePrefix = namePrefix;

        size_t functionCount = 0;
        for (size_t id = 0; id < result->Items.size(); ++id) {
            if (result->Items[id].IsOutOfLine()) {
                result->Items[id].Index = functionCount++;

                if (IsDumpExprsEnabled()) {
                    Cerr << Format("$%v := %v\n", id, InferName(id, DebugInfos, Items));
                }
            }
        }

        result->Functions.resize(functionCount, nullptr);

        return result;
    }

    void DumpArgs(const std::vector<size_t>& ids)
    {
        if (!IsDumpExprsEnabled()) {
            return;
        }
        for (size_t index = 0; index < ids.size(); ++index) {
            Cerr << Format("arg%v := %v\n", index, InferNameArg(ids[index], DebugInfos, Items));
        }
    }

    void DumpArgs(const std::vector<std::pair<size_t, bool>>& ids)
    {
        if (!IsDumpExprsEnabled()) {
            return;
        }
        for (size_t index = 0; index < ids.size(); ++index) {
            Cerr << Format("arg%v := %v\n", index, InferNameArg(ids[index].first, DebugInfos, Items));
        }
    }

};

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

    size_t Profile(TConstExpressionPtr expr, const TTableSchema& schema, TExpressionFragments* fragments);

protected:
    TCGVariables* Variables_;

    TConstFunctionProfilerMapPtr FunctionProfilers_;
};

size_t TExpressionProfiler::Profile(
    TConstExpressionPtr expr,
    const TTableSchema& schema,
    TExpressionFragments* fragments)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::LiteralExpr));

        auto savedId = id;
        id.AddString(ToString(TValue(literalExpr->Value)).c_str());

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(savedId);

            int index = Variables_->AddOpaque<TOwningValue>(literalExpr->Value);
            Fold(index);
            Fold(TValue(literalExpr->Value).Type == EValueType::Null);

            fragments->DebugInfos.emplace_back(expr, std::vector<size_t>());
            fragments->Items.emplace_back(
                MakeCodegenLiteralExpr(index, literalExpr->Type),
                expr->Type,
                true);
        }
        return emplaced.first->second;
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        auto indexInSchema = schema.GetColumnIndexOrThrow(referenceExpr->ColumnName);
        id.AddInteger(indexInSchema);

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(id);
            fragments->DebugInfos.emplace_back(expr, std::vector<size_t>());
            fragments->Items.emplace_back(
                MakeCodegenReferenceExpr(
                    indexInSchema,
                    referenceExpr->Type,
                    referenceExpr->ColumnName),
                expr->Type,
                true);
        }
        return emplaced.first->second;
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::FunctionExpr));
        id.AddString(functionExpr->FunctionName.c_str());

        std::vector<size_t> argIds;
        std::vector<EValueType> argumentTypes;
        std::vector<bool> literalArgs;
        for (const auto& argument : functionExpr->Arguments) {
            argIds.push_back(Profile(argument, schema, fragments));
            id.AddInteger(argIds.back());
            argumentTypes.push_back(argument->Type);
            literalArgs.push_back(argument->As<TLiteralExpression>() != nullptr);
        }

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(id);
            const auto& function = FunctionProfilers_->GetFunction(functionExpr->FunctionName);

            for (size_t argId : argIds) {
                ++fragments->Items[argId].UseCount;
            }
            fragments->DebugInfos.emplace_back(expr, argIds);
            fragments->Items.emplace_back(
                function->Profile(
                    Variables_,
                    std::move(argIds),
                    std::move(literalArgs),
                    std::move(argumentTypes),
                    functionExpr->Type,
                    "{" + InferName(functionExpr, true) + "}",
                    Id_),
                expr->Type);

        }
        return emplaced.first->second;
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::UnaryOpExpr));
        id.AddInteger(static_cast<int>(unaryOp->Opcode));

        size_t operand = Profile(unaryOp->Operand, schema, fragments);
        id.AddInteger(operand);

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(id);
            ++fragments->Items[operand].UseCount;
            fragments->DebugInfos.emplace_back(expr, std::vector<size_t>{operand});
            fragments->Items.emplace_back(MakeCodegenUnaryOpExpr(
                unaryOp->Opcode,
                operand,
                unaryOp->Type,
                "{" + InferName(unaryOp, true) + "}"),
                expr->Type);

        }
        return emplaced.first->second;
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        id.AddInteger(static_cast<int>(binaryOp->Opcode));

        size_t lhsOperand = Profile(binaryOp->Lhs, schema, fragments);
        id.AddInteger(lhsOperand);
        size_t rhsOperand = Profile(binaryOp->Rhs, schema, fragments);
        id.AddInteger(rhsOperand);

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(id);
            ++fragments->Items[lhsOperand].UseCount;
            ++fragments->Items[rhsOperand].UseCount;
            fragments->DebugInfos.emplace_back(expr, std::vector<size_t>{lhsOperand, rhsOperand});
            fragments->Items.emplace_back(MakeCodegenBinaryOpExpr(
                binaryOp->Opcode,
                lhsOperand,
                rhsOperand,
                binaryOp->Type,
                "{" + InferName(binaryOp, true) + "}"),
                expr->Type);
        }
        return emplaced.first->second;
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        id.AddInteger(static_cast<int>(EFoldingObjectType::InOpExpr));

        std::vector<size_t> argIds;
        for (const auto& argument : inOp->Arguments) {
            argIds.push_back(Profile(argument, schema, fragments));
            id.AddInteger(argIds.back());
        }

        for (const auto& value : inOp->Values) {
            id.AddString(ToString(value).c_str());
        }

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second) {
            Fold(id);
            for (size_t argId : argIds) {
                ++fragments->Items[argId].UseCount;
            }

            int index = Variables_->AddOpaque<TSharedRange<TRow>>(inOp->Values);
            fragments->DebugInfos.emplace_back(expr, argIds);
            fragments->Items.emplace_back(
                MakeCodegenInOpExpr(argIds, index),
                expr->Type);
        }
        return emplaced.first->second;
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
    size_t Profile(const TNamedItem& namedExpression, const TTableSchema& schema, TExpressionFragments* fragments);

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

        std::vector<size_t> groupExprIds;
        std::vector<size_t> aggregateExprIds;
        std::vector<TCodegenAggregate> codegenAggregates;

        std::vector<EValueType> keyTypes;

        TExpressionFragments groupFragments;
        for (const auto& groupItem : groupClause->GroupItems) {
            groupExprIds.push_back(Profile(groupItem, schema, &groupFragments));
            keyTypes.push_back(groupItem.Expression->Type);
        }

        TExpressionFragments aggregateFragments;
        for (const auto& aggregateItem : groupClause->AggregateItems) {
            Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
            Fold(aggregateItem.AggregateFunction.c_str());
            Fold(aggregateItem.Name.c_str());

            const auto& aggregate = AggregateProfilers_->GetAggregate(aggregateItem.AggregateFunction);

            if (!isMerge) {
                aggregateExprIds.push_back(Profile(aggregateItem, schema, &aggregateFragments));
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

        auto aggregateFragmentInfos = aggregateFragments.ToFragmentInfos("aggregateExpression");
        aggregateFragments.DumpArgs(aggregateExprIds);

        auto aggregate = MakeCodegenEvaluateAggregateArgs(
            keySize,
            aggregateFragmentInfos,
            aggregateExprIds);

        auto update = MakeCodegenAggregateUpdate(
            codegenAggregates,
            keySize,
            isMerge);

        auto finalize = MakeCodegenAggregateFinalize(
            codegenAggregates,
            keySize);

        auto groupFragmentsInfos = groupFragments.ToFragmentInfos("groupExpression");
        groupFragments.DumpArgs(groupExprIds);

        intermediateSlot = MakeCodegenGroupOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            initialize,
            MakeCodegenEvaluateGroups(
                groupFragmentsInfos,
                groupExprIds),
            aggregate,
            update,
            keyTypes,
            isMerge,
            keySize + codegenAggregates.size(),
            groupClause->TotalsMode != ETotalsMode::None);

        Fold(static_cast<int>(EFoldingObjectType::TotalsMode));
        Fold(static_cast<int>(groupClause->TotalsMode));

        schema = groupClause->GetTableSchema(query->IsFinal);

        TCodegenFragmentInfosPtr havingFragmentsInfos;

        size_t havingPredicateId;
        if (query->HavingClause) {
            TExpressionFragments havingExprFragments;
            havingPredicateId = TExpressionProfiler::Profile(query->HavingClause, schema, &havingExprFragments);

            havingFragmentsInfos = havingExprFragments.ToFragmentInfos("havingExpression");
            havingExprFragments.DumpArgs(std::vector<size_t>{havingPredicateId});
        }

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

            if (query->HavingClause && !IsTrue(query->HavingClause)) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                intermediateSlot = MakeCodegenFilterOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot,
                    havingFragmentsInfos,
                    havingPredicateId);
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
                    New<TCodegenFragmentInfos>(),
                    std::vector<size_t>(),
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

                if (groupClause->TotalsMode == ETotalsMode::BeforeHaving && query->HavingClause && !IsTrue(query->HavingClause)) {
                    Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                    totalsSlot = MakeCodegenFilterOp(
                        codegenSource,
                        slotCount,
                        totalsSlot,
                        havingFragmentsInfos,
                        havingPredicateId);
                }
            }
        }
        MakeCodegenFragmentBodies(codegenSource, groupFragmentsInfos);
        MakeCodegenFragmentBodies(codegenSource, aggregateFragmentInfos);
        if (havingFragmentsInfos) {
            MakeCodegenFragmentBodies(codegenSource, havingFragmentsInfos);
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

        std::vector<size_t> orderExprIds;
        std::vector<bool> isDesc;
        std::vector<EValueType> orderColumnTypes;
        TExpressionFragments orderExprFragments;
        for (const auto& item : orderClause->OrderItems) {
            orderExprIds.push_back(TExpressionProfiler::Profile(item.first, schema, &orderExprFragments));
            Fold(item.second);
            isDesc.push_back(item.second);
            orderColumnTypes.push_back(item.first->Type);
        }

        auto orderFragmentsInfos = orderExprFragments.ToFragmentInfos("orderExpression");
        orderExprFragments.DumpArgs(orderExprIds);

        auto schemaTypes = GetTypesFromSchema(schema);

        for (auto type : schemaTypes) {
            Fold(static_cast<ui16>(type));
        }

        finalSlot = MakeCodegenOrderOp(
            codegenSource,
            slotCount,
            finalSlot,
            orderFragmentsInfos,
            orderExprIds,
            std::move(orderColumnTypes),
            schemaTypes,
            std::move(isDesc));
        MakeCodegenFragmentBodies(codegenSource, orderFragmentsInfos);
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        std::vector<size_t> projectExprIds;
        TExpressionFragments projectExprFragments;
        for (const auto& item : projectClause->Projections) {
            projectExprIds.push_back(Profile(item, schema, &projectExprFragments));
        }

        auto projectFragmentsInfos = projectExprFragments.ToFragmentInfos("projectExpression");
        projectExprFragments.DumpArgs(projectExprIds);

        finalSlot = MakeCodegenProjectOp(codegenSource, slotCount, finalSlot, projectFragmentsInfos, projectExprIds);
        totalsSlot = MakeCodegenProjectOp(codegenSource, slotCount, totalsSlot, projectFragmentsInfos, projectExprIds);

        MakeCodegenFragmentBodies(codegenSource, projectFragmentsInfos);

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
    TSchemaProfiler::Profile(schema);

    size_t currentSlot = MakeCodegenScanOp(codegenSource, slotCount);

    auto whereClause = query->WhereClause;

    for (const auto& joinClause : query->JoinClauses) {
        Fold(static_cast<int>(EFoldingObjectType::JoinOp));

        std::vector<std::pair<size_t, bool>> selfKeys;

        TExpressionFragments selfEquationFragments;
        for (const auto& column : joinClause->SelfEquations) {
            TConstExpressionPtr expression;
            bool isEvaluated;
            std::tie(expression, isEvaluated) = column;

            const auto& expressionSchema = isEvaluated ? joinClause->OriginalSchema : schema;
            selfKeys.emplace_back(
                TExpressionProfiler::Profile(expression, expressionSchema, &selfEquationFragments),
                isEvaluated);
        }

        TConstExpressionPtr selfFilter;
        std::tie(selfFilter, whereClause) = SplitPredicateByColumnSubset(whereClause, schema);

        if (selfFilter && !IsTrue(selfFilter)) {
            Fold(static_cast<int>(EFoldingObjectType::FilterOp));
            TExpressionFragments filterExprFragments;
            size_t predicateId = TExpressionProfiler::Profile(selfFilter, schema, &filterExprFragments);
            auto fragmentInfos = filterExprFragments.ToFragmentInfos("selfFilter");
            filterExprFragments.DumpArgs(std::vector<size_t>{predicateId});

            currentSlot = MakeCodegenFilterOp(
                codegenSource,
                slotCount,
                currentSlot,
                fragmentInfos,
                predicateId);
            MakeCodegenFragmentBodies(codegenSource, fragmentInfos);
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

        Fold(index);

        YCHECK(joinClause->CommonKeyPrefix < 1000);

        Fold(joinClause->CommonKeyPrefix);

        auto fragmentInfos = selfEquationFragments.ToFragmentInfos("selfEquation");
        selfEquationFragments.DumpArgs(selfKeys);

        currentSlot = MakeCodegenJoinOp(
            codegenSource,
            slotCount,
            currentSlot,
            index,
            fragmentInfos,
            selfKeys,
            joinClause->CommonKeyPrefix);
        MakeCodegenFragmentBodies(codegenSource, fragmentInfos);

        schema = joinClause->GetTableSchema(schema);
        TSchemaProfiler::Profile(schema);
    }

    if (whereClause && !IsTrue(whereClause)) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        TExpressionFragments filterExprFragments;
        size_t predicateId = TExpressionProfiler::Profile(whereClause, schema, &filterExprFragments);

        auto fragmentInfos = filterExprFragments.ToFragmentInfos("filterExpression");
        filterExprFragments.DumpArgs(std::vector<size_t>{predicateId});

        currentSlot = MakeCodegenFilterOp(
            codegenSource,
            slotCount,
            currentSlot,
            fragmentInfos,
            predicateId);
        MakeCodegenFragmentBodies(codegenSource, fragmentInfos);
    }

    size_t dummySlot = (*slotCount)++;
    Profile(codegenSource, query, slotCount, dummySlot, currentSlot, dummySlot, schema, false);
}

void TQueryProfiler::Profile(TCodegenSource* codegenSource, TConstFrontQueryPtr query, size_t* slotCount)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();
    TSchemaProfiler::Profile(schema);

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

size_t TQueryProfiler::Profile(
    const TNamedItem& namedExpression,
    const TTableSchema& schema,
    TExpressionFragments* fragments)
{
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));

    size_t resultId = TExpressionProfiler::Profile(namedExpression.Expression, schema, fragments);
    ++fragments->Items[resultId].UseCount;

    return resultId;
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
    TExpressionFragments fragments;
    auto exprId = profiler.Profile(expr, schema, &fragments);

    return [
            fragmentInfos = fragments.ToFragmentInfos("fragment"),
            MOVE(exprId)
        ] () {
            return CodegenStandaloneExpression(fragmentInfos, exprId);
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
            slotCount
        ] () {
            return CodegenEvaluate(&codegenSource, slotCount);
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

