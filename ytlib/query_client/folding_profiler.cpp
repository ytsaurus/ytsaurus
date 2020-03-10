#include "folding_profiler.h"
#include "cg_fragment_compiler.h"
#include "functions.h"
#include "functions_cg.h"
#include "query_helpers.h"
#include "llvm_folding_set.h"
#include "helpers.h"

namespace NYT::NQueryClient {
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
    (WriteOp)

    (LiteralExpr)
    (ReferenceExpr)
    (FunctionExpr)
    (UnaryOpExpr)
    (BinaryOpExpr)
    (InExpr)
    (BetweenExpr)
    (TransformExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
    (IsFinal)
    (IsMerge)
    (TotalsMode)
);

//! Computes a strong structural hash used to cache query fragments.

////////////////////////////////////////////////////////////////////////////////

std::vector<EValueType> GetTypesFromSchema(const TTableSchema& tableSchema)
{
    std::vector<EValueType> result;

    for (const auto& column : tableSchema.Columns()) {
        result.push_back(column.GetPhysicalType());
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
        Fold(static_cast<ui16>(column.GetPhysicalType()));
        Fold(column.Name().c_str());
        int aux = (column.Expression() ? 1 : 0) | ((column.Aggregate() ? 1 : 0) << 1);
        Fold(aux);
        if (column.Expression()) {
            Fold(column.Expression()->c_str());
        }
        if (column.Aggregate()) {
            Fold(column.Aggregate()->c_str());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDebugInfo
{
    TConstExpressionPtr Expr;
    std::vector<size_t> Args;
    std::optional<size_t> ExtraArg;

    TDebugInfo(
        const TConstExpressionPtr& expr,
        const std::vector<size_t>& args,
        const std::optional<size_t>& extraArg = std::nullopt)
        : Expr(expr)
        , Args(args)
        , ExtraArg(extraArg)
    { }

};

struct TExpressionFragmentPrinter
    : TAbstractExpressionPrinter<TExpressionFragmentPrinter, size_t, size_t>
{
    using TBase = TAbstractExpressionPrinter<TExpressionFragmentPrinter, size_t, size_t>;

    const std::vector<TDebugInfo>& DebugExpressions;
    const std::vector<TCodegenFragmentInfo>& Expressions;

    TExpressionFragmentPrinter(
        TStringBuilderBase* builder,
        const std::vector<TDebugInfo>& debugExpressions,
        const std::vector<TCodegenFragmentInfo>& expressions)
        : TBase(builder, false)
        , DebugExpressions(debugExpressions)
        , Expressions(expressions)
    { }

    const TExpression* GetExpression(size_t id)
    {
        return &*DebugExpressions[id].Expr;
    }

    void InferNameArg(size_t id)
    {
        if (Expressions[id].IsOutOfLine()) {
            Builder->AppendString(Format("$%v", id));
        } else {
            Visit(id, id);
        }
    }

    void OnOperand(const TUnaryOpExpression* unaryExpr, size_t id)
    {
        auto operandId = DebugExpressions[id].Args[0];
        InferNameArg(operandId);
    }

    void OnLhs(const TBinaryOpExpression* binaryExpr, size_t id)
    {
        auto lhsId = DebugExpressions[id].Args[0];
        InferNameArg(lhsId);
    }

    void OnRhs(const TBinaryOpExpression* binaryExpr, size_t id)
    {
        auto rhsId = DebugExpressions[id].Args[1];
        InferNameArg(rhsId);
    }

    void OnDefaultExpression(const TTransformExpression* transformExpr, size_t id)
    {
        const auto& defaultExpr = DebugExpressions[id].ExtraArg;
        if (defaultExpr) {
            Builder->AppendString(", ");
            InferNameArg(*defaultExpr);
        }
    }

    template <class T>
    void OnArguments(const T* expr, size_t id)
    {
        bool needComma = false;
        for (const auto& argument : DebugExpressions[id].Args) {
            if (needComma) {
                Builder->AppendString(", ");
            }
            InferNameArg(argument);
            needComma = true;
        }
    }

    void OnReference(const TReferenceExpression* referenceExpr, size_t id)
    {
        auto columnName = referenceExpr->ColumnName;
        if (columnName.size() > 40) {
            columnName.resize(40);
            columnName.Transform([] (size_t index, char c) {
                if (c == '(' || c == ')') {
                    return '_';
                }
                return c;
            });

            Builder->AppendString(
                Format("[%x%v]", FarmFingerprint(
                    referenceExpr->ColumnName.data(),
                    referenceExpr->ColumnName.size()),
                columnName));
        } else {
            Builder->AppendString(Format("[%v]", columnName));
        }
    }

};

static bool IsDumpExprsEnabled()
{
    static bool result = (getenv("DUMP_EXPRS") != nullptr);
    return result;
}

struct TExpressionFragments
{
    std::vector<TCodegenFragmentInfo> Items;
    THashMap<llvm::FoldingSetNodeID, size_t> Fingerprints;
    std::vector<TDebugInfo> DebugInfos;

    TCodegenFragmentInfosPtr ToFragmentInfos(const TString& namePrefix)
    {
        if (IsDumpExprsEnabled()) {
            Cerr << "\n" << namePrefix << "\n";
        }

        auto result = New<TCodegenFragmentInfos>();
        result->Items.assign(Items.begin(), Items.end());
        result->NamePrefix = namePrefix;

        TStringBuilder builder;
        TExpressionFragmentPrinter expressionPrinter(
            &builder,
            DebugInfos,
            Items);

        size_t functionCount = 0;
        for (size_t id = 0; id < result->Items.size(); ++id) {
            if (result->Items[id].IsOutOfLine()) {
                result->Items[id].Index = functionCount++;

                if (IsDumpExprsEnabled()) {
                    expressionPrinter.Visit(id, id);

                    Cerr << Format(
                        "$%v %v:= %v\n",
                        id,
                        result->Items[id].Nullable ? "nullable " : "",
                        builder.Flush());
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

        TStringBuilder builder;
        TExpressionFragmentPrinter expressionPrinter(
            &builder,
            DebugInfos,
            Items);

        for (size_t index = 0; index < ids.size(); ++index) {
            expressionPrinter.InferNameArg(ids[index]);
            Cerr << Format("arg%v := %v\n", index, builder.Flush());
        }
    }

    void DumpArgs(const std::vector<std::pair<size_t, bool>>& ids)
    {
        if (!IsDumpExprsEnabled()) {
            return;
        }

        TStringBuilder builder;
        TExpressionFragmentPrinter expressionPrinter(
            &builder,
            DebugInfos,
            Items);

        for (size_t index = 0; index < ids.size(); ++index) {
            expressionPrinter.InferNameArg(ids[index].first);
            Cerr << Format("arg%v := %v\n", index, builder.Flush());
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
        , ComparerManager_(MakeComparerManager())
    {
        YT_VERIFY(Variables_);
    }

    size_t Profile(
        const TConstExpressionPtr& expr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated = false);

private:
    size_t Profile(
        const TLiteralExpression* literalExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TReferenceExpression* referenceExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TFunctionExpression* functionExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TUnaryOpExpression* unaryOp,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TBinaryOpExpression* binaryOp,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TInExpression* inExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TBetweenExpression* betweenExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

    size_t Profile(
        const TTransformExpression* transformExpr,
        const TTableSchema& schema,
        TExpressionFragments* fragments,
        bool isIsolated);

protected:
    TCGVariables* const Variables_;
    const TConstFunctionProfilerMapPtr FunctionProfilers_;
    const TComparerManagerPtr ComparerManager_;
};

size_t TExpressionProfiler::Profile(
    const TLiteralExpression* literalExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(literalExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::LiteralExpr));

    auto savedId = id;
    id.AddString(ToString(TValue(literalExpr->Value)).c_str());

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(savedId);

        int index = Variables_->AddLiteralValue(literalExpr->Value);
        Fold(index);

        bool nullable = TValue(literalExpr->Value).Type == EValueType::Null;
        Fold(nullable);

        fragments->DebugInfos.emplace_back(literalExpr, std::vector<size_t>());
        fragments->Items.emplace_back(
            MakeCodegenLiteralExpr(index, nullable, literalExpr->Type),
            literalExpr->Type,
            nullable,
            true);
    }

    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TReferenceExpression* referenceExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(referenceExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::ReferenceExpr));
    auto indexInSchema = schema.GetColumnIndexOrThrow(referenceExpr->ColumnName);
    id.AddInteger(indexInSchema);

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(id);
        fragments->DebugInfos.emplace_back(referenceExpr, std::vector<size_t>());
        fragments->Items.emplace_back(
            MakeCodegenReferenceExpr(
                indexInSchema,
                referenceExpr->Type,
                referenceExpr->ColumnName),
            referenceExpr->Type,
            true,
            true);
    }
    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TFunctionExpression* functionExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(functionExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::FunctionExpr));
    id.AddString(functionExpr->FunctionName.c_str());

    std::vector<size_t> argIds;
    std::vector<EValueType> argumentTypes;
    auto literalArgs = std::make_unique<bool[]>(functionExpr->Arguments.size());
    size_t index = 0;
    for (const auto& argument : functionExpr->Arguments) {
        argIds.push_back(Profile(argument, schema, fragments, isIsolated));
        id.AddInteger(argIds.back());
        argumentTypes.push_back(argument->Type);
        literalArgs[index++] = argument->As<TLiteralExpression>() != nullptr;
    }

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(id);
        const auto& function = FunctionProfilers_->GetFunction(functionExpr->FunctionName);

        std::vector<bool> nullableArgs;
        for (size_t argId : argIds) {
            ++fragments->Items[argId].UseCount;
            nullableArgs.push_back(fragments->Items[argId].Nullable);
        }

        fragments->DebugInfos.emplace_back(functionExpr, argIds);
        fragments->Items.emplace_back(
            function->Profile(
                Variables_,
                std::move(argIds),
                std::move(literalArgs),
                std::move(argumentTypes),
                functionExpr->Type,
                "{" + InferName(functionExpr, true) + "}",
                Id_),
            functionExpr->Type,
            function->IsNullable(nullableArgs));
    }
    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TUnaryOpExpression* unaryOp,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(unaryOp->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::UnaryOpExpr));
    id.AddInteger(static_cast<int>(unaryOp->Opcode));

    size_t operand = Profile(unaryOp->Operand, schema, fragments, isIsolated);
    id.AddInteger(operand);

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(id);
        ++fragments->Items[operand].UseCount;
        fragments->DebugInfos.emplace_back(unaryOp, std::vector<size_t>{operand});
        fragments->Items.emplace_back(MakeCodegenUnaryOpExpr(
            unaryOp->Opcode,
            operand,
            unaryOp->Type,
            "{" + InferName(unaryOp, true) + "}"),
            unaryOp->Type,
            fragments->Items[operand].Nullable);
    }
    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TBinaryOpExpression* binaryOp,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(binaryOp->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        id.AddInteger(static_cast<int>(binaryOp->Opcode));

        size_t lhsOperand = Profile(binaryOp->Lhs, schema, fragments, isIsolated);
        id.AddInteger(lhsOperand);
        size_t rhsOperand = Profile(binaryOp->Rhs, schema, fragments, isIsolated);
        id.AddInteger(rhsOperand);

        auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
        if (emplaced.second || isIsolated) {
            Fold(id);
            ++fragments->Items[lhsOperand].UseCount;
            ++fragments->Items[rhsOperand].UseCount;
            fragments->DebugInfos.emplace_back(binaryOp, std::vector<size_t>{lhsOperand, rhsOperand});
            bool nullable = IsRelationalBinaryOp(binaryOp->Opcode)
                ? false
                : fragments->Items[lhsOperand].Nullable || fragments->Items[rhsOperand].Nullable;
            fragments->Items.emplace_back(MakeCodegenBinaryOpExpr(
                binaryOp->Opcode,
                lhsOperand,
                rhsOperand,
                binaryOp->Type,
                "{" + InferName(binaryOp, true) + "}"),
                binaryOp->Type,
                nullable);
        }
        return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TInExpression* inExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(inExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::InExpr));

    std::vector<size_t> argIds;
    for (const auto& argument : inExpr->Arguments) {
        argIds.push_back(Profile(argument, schema, fragments, isIsolated));
        id.AddInteger(argIds.back());
    }

    auto savedId = id;
    for (const auto& value : inExpr->Values) {
        id.AddString(ToString(value).c_str());
    }

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(savedId);
        for (size_t argId : argIds) {
            ++fragments->Items[argId].UseCount;
        }

        int index = Variables_->AddOpaque<TSharedRange<TRow>>(inExpr->Values);
        int hashtableIndex = Variables_->AddOpaque<std::unique_ptr<TLookupRows>>();
        fragments->DebugInfos.emplace_back(inExpr, argIds);
        fragments->Items.emplace_back(
            MakeCodegenInExpr(argIds, index, hashtableIndex, ComparerManager_),
            inExpr->Type,
            false);
    }
    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TBetweenExpression* betweenExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(betweenExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::BetweenExpr));

    std::vector<size_t> argIds;
    for (const auto& argument : betweenExpr->Arguments) {
        argIds.push_back(Profile(argument, schema, fragments, isIsolated));
        id.AddInteger(argIds.back());
    }

    auto savedId = id;
    for (const auto& range : betweenExpr->Ranges) {
        id.AddString(ToString(range.first).c_str());
        id.AddString(ToString(range.second).c_str());
    }

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(savedId);
        for (size_t argId : argIds) {
            ++fragments->Items[argId].UseCount;
        }

        int index = Variables_->AddOpaque<TSharedRange<TRowRange>>(betweenExpr->Ranges);
        fragments->DebugInfos.emplace_back(betweenExpr, argIds);
        fragments->Items.emplace_back(
            MakeCodegenBetweenExpr(argIds, index, ComparerManager_),
            betweenExpr->Type,
            false);
    }
    return emplaced.first->second;
}

size_t TExpressionProfiler::Profile(
    const TTransformExpression* transformExpr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<ui16>(transformExpr->Type));

    id.AddInteger(static_cast<int>(EFoldingObjectType::TransformExpr));

    std::vector<size_t> argIds;
    for (const auto& argument : transformExpr->Arguments) {
        argIds.push_back(Profile(argument, schema, fragments, isIsolated));
        id.AddInteger(argIds.back());
    }

    std::optional<size_t> defaultExprId;
    if (const auto& defaultExpression = transformExpr->DefaultExpression) {
        defaultExprId = Profile(defaultExpression, schema, fragments, isIsolated);
        id.AddInteger(*defaultExprId);
    }

    auto savedId = id;
    for (const auto& value : transformExpr->Values) {
        id.AddString(ToString(value).c_str());
    }

    auto emplaced = fragments->Fingerprints.emplace(id, fragments->Items.size());
    if (emplaced.second || isIsolated) {
        Fold(savedId);
        for (size_t argId : argIds) {
            ++fragments->Items[argId].UseCount;
        }

        bool nullable = true;

        if (defaultExprId) {
            ++fragments->Items[*defaultExprId].UseCount;

            nullable = false;
            nullable |= fragments->Items[*defaultExprId].Nullable;

            for (TRow row : transformExpr->Values) {
                nullable |= row[argIds.size()].Type == EValueType::Null;
            }
        }

        int index = Variables_->AddOpaque<TSharedRange<TRow>>(transformExpr->Values);
        int hashtableIndex = Variables_->AddOpaque<std::unique_ptr<TLookupRows>>();

        fragments->DebugInfos.emplace_back(transformExpr, argIds, defaultExprId);
        fragments->Items.emplace_back(
            MakeCodegenTransformExpr(argIds, defaultExprId, index, hashtableIndex, transformExpr->Type, ComparerManager_),
            transformExpr->Type,
            nullable);
    }
    return emplaced.first->second;

}

size_t TExpressionProfiler::Profile(
    const TConstExpressionPtr& expr,
    const TTableSchema& schema,
    TExpressionFragments* fragments,
    bool isIsolated)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return Profile(literalExpr, schema, fragments, isIsolated);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return Profile(referenceExpr, schema, fragments, isIsolated);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        return Profile(functionExpr, schema, fragments, isIsolated);
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        return Profile(unaryOp, schema, fragments, isIsolated);
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        return Profile(binaryOp, schema, fragments, isIsolated);
    } else if (auto inExpr = expr->As<TInExpression>()) {
        return Profile(inExpr, schema, fragments, isIsolated);
    } else if (auto betweenExpr = expr->As<TBetweenExpression>()) {
        return Profile(betweenExpr, schema, fragments, isIsolated);
    } else if (auto transformExpr = expr->As<TTransformExpression>()) {
        return Profile(transformExpr, schema, fragments, isIsolated);
    }

    YT_ABORT();
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
        const TConstBaseQueryPtr& query,
        size_t* slotCount,
        size_t finalSlot,
        size_t intermediateSlot,
        size_t totalsSlot,
        TTableSchema schema,
        bool isMerge);

    void Profile(
        TCodegenSource* codegenSource,
        const TConstQueryPtr& query,
        size_t* slotCount,
        TJoinSubqueryProfiler joinProfiler,
        bool useMultijoin);

    void Profile(
        TCodegenSource* codegenSource,
        const TConstFrontQueryPtr& query,
        size_t* slotCount);

protected:
    const TConstAggregateProfilerMapPtr AggregateProfilers_;

    size_t Profile(
        const TNamedItem& namedExpression,
        const TTableSchema& schema,
        TExpressionFragments* fragments);
};

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstBaseQueryPtr& query,
    size_t* slotCount,
    size_t finalSlot,
    size_t intermediateSlot,
    size_t totalsSlot,
    TTableSchema schema,
    bool isMerge)
{
    size_t dummySlot = (*slotCount)++;

    bool isFinal = query->IsFinal;

    Fold(static_cast<int>(EFoldingObjectType::IsFinal));
    Fold(static_cast<int>(isFinal));
    Fold(static_cast<int>(EFoldingObjectType::IsMerge));
    Fold(static_cast<int>(isMerge));

    if (auto groupClause = query->GroupClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::GroupOp));
        Fold(static_cast<int>(groupClause->CommonPrefixWithPrimaryKey));

        std::vector<EValueType> keyTypes;
        std::vector<EValueType> stateTypes;

        std::vector<size_t> groupExprIds;
        std::vector<size_t> aggregateExprIds;
        std::vector<TCodegenAggregate> codegenAggregates;

        TExpressionFragments expressionFragments;

        TExpressionFragments groupFragments;
        for (const auto& groupItem : groupClause->GroupItems) {
            groupExprIds.push_back(Profile(groupItem, schema, &expressionFragments));
            keyTypes.push_back(groupItem.Expression->Type);
        }

        for (const auto& aggregateItem : groupClause->AggregateItems) {
            Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
            Fold(aggregateItem.AggregateFunction.c_str());
            Fold(aggregateItem.Name.c_str());

            const auto& aggregate = AggregateProfilers_->GetAggregate(aggregateItem.AggregateFunction);

            if (!isMerge) {
                aggregateExprIds.push_back(Profile(aggregateItem, schema, &expressionFragments));
            }
            codegenAggregates.push_back(aggregate->Profile(
                aggregateItem.Expression->Type,
                aggregateItem.StateType,
                aggregateItem.ResultType,
                aggregateItem.Name,
                Id_));
            stateTypes.push_back(aggregateItem.StateType);
        }

        auto fragmentInfos = expressionFragments.ToFragmentInfos("groupExpression");
        expressionFragments.DumpArgs(aggregateExprIds);
        expressionFragments.DumpArgs(groupExprIds);

        // If group key contains primary key prefix, full grouped rowset is not keeped till the end but flushed
        // every time prefix changes (scan is ordered by primary key, bottom queries are always evaluated along
        // each tablet). Grouped rows with inner primary key prefix are transfered to final slot (they are
        // disjoint). Grouped rows with boundary primary key prefix (with respect to tablet) are transfered to
        // intermediate slot and needs final grouping.

        size_t newFinalSlot;
        std::tie(intermediateSlot, newFinalSlot) = MakeCodegenGroupOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            fragmentInfos,
            groupExprIds,
            aggregateExprIds,
            codegenAggregates,
            keyTypes,
            stateTypes,
            isMerge,
            groupClause->TotalsMode != ETotalsMode::None,
            groupClause->CommonPrefixWithPrimaryKey,
            ComparerManager_);

        Fold(static_cast<int>(EFoldingObjectType::TotalsMode));
        Fold(static_cast<int>(groupClause->TotalsMode));

        schema = groupClause->GetTableSchema(query->IsFinal);

        TCodegenFragmentInfosPtr havingFragmentsInfos;

        size_t havingPredicateId;
        bool addHaving = query->HavingClause && !IsTrue(query->HavingClause);

        if (addHaving) {
            TExpressionFragments havingExprFragments;
            havingPredicateId = TExpressionProfiler::Profile(query->HavingClause, schema, &havingExprFragments);

            havingFragmentsInfos = havingExprFragments.ToFragmentInfos("havingExpression");
            havingExprFragments.DumpArgs(std::vector<size_t>{havingPredicateId});
        }

        // COMPAT(lukyan)
        if (isFinal || query->UseDisjointGroupBy) {
            // Boundary segments are also final
            newFinalSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                newFinalSlot);

             intermediateSlot = dummySlot;
        } else if (isMerge) {
            intermediateSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                newFinalSlot);

             newFinalSlot = dummySlot;
        }

        size_t keySize = groupClause->GroupItems.size();

        if (!isMerge || isFinal) {
            if (addHaving && groupClause->TotalsMode == ETotalsMode::AfterHaving) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));

                // Finalizes row to evaluate predicate and filters source values.
                newFinalSlot = MakeCodegenFilterFinalizedOp(
                    codegenSource,
                    slotCount,
                    newFinalSlot,
                    havingFragmentsInfos,
                    havingPredicateId,
                    keySize,
                    codegenAggregates,
                    stateTypes);
            }

            if (groupClause->TotalsMode != ETotalsMode::None) {
                size_t totalsSlotNew;
                std::tie(totalsSlotNew, newFinalSlot) = MakeCodegenDuplicateOp(
                    codegenSource,
                    slotCount,
                    newFinalSlot);

                if (isMerge) {
                    totalsSlot = MakeCodegenMergeOp(
                        codegenSource,
                        slotCount,
                        totalsSlot,
                        totalsSlotNew);
                } else {
                    totalsSlot = totalsSlotNew;
                }
            }

            newFinalSlot = MakeCodegenFinalizeOp(
                codegenSource,
                slotCount,
                newFinalSlot,
                keySize,
                codegenAggregates,
                stateTypes);

            if (addHaving && groupClause->TotalsMode != ETotalsMode::AfterHaving) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                newFinalSlot = MakeCodegenFilterOp(
                    codegenSource,
                    slotCount,
                    newFinalSlot,
                    havingFragmentsInfos,
                    havingPredicateId);
            }

            if (isMerge) {
                finalSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    newFinalSlot,
                    finalSlot);
            } else {
                finalSlot = newFinalSlot;
            }
        }

        if (groupClause->TotalsMode != ETotalsMode::None) {
            totalsSlot = MakeCodegenGroupTotalsOp(
                codegenSource,
                slotCount,
                totalsSlot,
                codegenAggregates,
                keyTypes,
                stateTypes);

            if (isFinal) {
                totalsSlot = MakeCodegenFinalizeOp(
                    codegenSource,
                    slotCount,
                    totalsSlot,
                    keySize,
                    codegenAggregates,
                    stateTypes);
            }
        }

        MakeCodegenFragmentBodies(codegenSource, fragmentInfos);
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

    intermediateSlot = MakeCodegenOnceOp(codegenSource, slotCount, intermediateSlot);
    finalSlot = MakeCodegenOnceOp(codegenSource, slotCount, finalSlot);

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
            std::move(isDesc),
            ComparerManager_);
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

        // FIXME(lukyan): Do not generate ProjectOp two times.
        finalSlot = MakeCodegenProjectOp(codegenSource, slotCount, finalSlot, projectFragmentsInfos, projectExprIds);
        totalsSlot = MakeCodegenProjectOp(codegenSource, slotCount, totalsSlot, projectFragmentsInfos, projectExprIds);

        MakeCodegenFragmentBodies(codegenSource, projectFragmentsInfos);

        schema = projectClause->GetTableSchema();
    }

    bool considerLimit = query->IsOrdered() && !query->GroupClause;
    Fold(static_cast<int>(considerLimit));
    if (considerLimit) {
        int offsetId = Variables_->AddOpaque<size_t>(query->Offset);
        int limitId = Variables_->AddOpaque<size_t>(query->Limit);

        finalSlot = MakeCodegenOffsetLimiterOp(codegenSource, slotCount, finalSlot, offsetId, limitId);
    }

    size_t resultRowSize = schema.GetColumnCount();

    if (!isFinal) {
        finalSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                finalSlot,
                resultRowSize,
                EStreamTag::Final);

        totalsSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                totalsSlot,
                resultRowSize,
                EStreamTag::Totals);

        intermediateSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                resultRowSize,
                EStreamTag::Intermediate);

        ++resultRowSize;
    }

    size_t resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, finalSlot, totalsSlot);
    resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, resultSlot, intermediateSlot);

    //resultSlot = MakeCodegenOnceOp(codegenSource, slotCount, resultSlot);

    Fold(static_cast<int>(EFoldingObjectType::WriteOp));
    MakeCodegenWriteOp(codegenSource, resultSlot, resultRowSize);
}

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstQueryPtr& query,
    size_t* slotCount,
    TJoinSubqueryProfiler joinProfiler,
    bool useMultijoin)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();
    TSchemaProfiler::Profile(schema);

    size_t currentSlot = MakeCodegenScanOp(codegenSource, slotCount);

    auto whereClause = query->WhereClause;

    auto Logger = MakeQueryLogger(query);

    if (useMultijoin) {
        std::vector<size_t> joinGroups = GetJoinGroups(query->JoinClauses, schema);
        if (!joinGroups.empty()) {
            YT_LOG_DEBUG("Join groups: [%v]", JoinToString(joinGroups));
        }

        size_t joinIndex = 0;
        for (size_t joinGroupSize : joinGroups) {
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

            Fold(static_cast<int>(EFoldingObjectType::JoinOp));
            TExpressionFragments equationFragments;

            std::vector<TSingleJoinCGParameters> parameters;

            size_t joinBatchSize = std::numeric_limits<size_t>::max();

            if (query->IsOrdered()) {
                joinBatchSize = query->Offset + query->Limit;
            }

            TMultiJoinParameters joinParameters;

            std::vector<TString> selfColumnNames;

            auto lastSchema = schema;
            for (; joinGroupSize > 0; ++joinIndex, --joinGroupSize) {
                const auto& joinClause = query->JoinClauses[joinIndex];

                std::vector<std::pair<size_t, bool>> selfKeys;
                std::vector<EValueType> lookupKeyTypes;
                for (const auto& column : joinClause->SelfEquations) {
                    TConstExpressionPtr expression;
                    bool isEvaluated;
                    std::tie(expression, isEvaluated) = column;

                    const auto& expressionSchema = isEvaluated ? joinClause->Schema.Original : schema;

                    selfKeys.emplace_back(
                        TExpressionProfiler::Profile(
                            expression,
                            expressionSchema,
                            &equationFragments,
                            isEvaluated),
                        isEvaluated);
                    lookupKeyTypes.push_back(expression->Type);
                }

                TSingleJoinCGParameters codegenParameters{
                    selfKeys,
                    joinClause->CommonKeyPrefix,
                    joinClause->ForeignKeyPrefix,
                    lookupKeyTypes};

                Fold(joinClause->CommonKeyPrefix);
                Fold(joinClause->ForeignKeyPrefix);

                parameters.push_back(codegenParameters);

                TSingleJoinParameters singeJoinParameters;

                {
                    const auto& foreignEquations = joinClause->ForeignEquations;

                    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
                    auto subquery = New<TQuery>();

                    subquery->Schema.Original = joinClause->Schema.Original;
                    subquery->Schema.Mapping = joinClause->Schema.Mapping;

                    // (join key... , other columns...)
                    auto projectClause = New<TProjectClause>();
                    std::vector<TConstExpressionPtr> joinKeyExprs;

                    for (const auto& column : foreignEquations) {
                        projectClause->AddProjection(column, InferName(column));
                    }

                    subquery->ProjectClause = projectClause;
                    subquery->WhereClause = joinClause->Predicate;

                    selfColumnNames = joinClause->SelfJoinedColumns;

                    auto foreignColumnNames = joinClause->ForeignJoinedColumns;
                    std::sort(foreignColumnNames.begin(), foreignColumnNames.end());

                    auto joinRenamedTableColumns = joinClause->GetRenamedSchema().Columns();

                    std::vector<size_t> foreignColumns;
                    for (size_t index = 0; index < joinRenamedTableColumns.size(); ++index) {
                        if (std::binary_search(
                            foreignColumnNames.begin(),
                            foreignColumnNames.end(),
                            joinRenamedTableColumns[index].Name()))
                        {
                            foreignColumns.push_back(projectClause->Projections.size());

                            projectClause->AddProjection(
                                New<TReferenceExpression>(
                                    joinRenamedTableColumns[index].GetPhysicalType(),
                                    joinRenamedTableColumns[index].Name()),
                                joinRenamedTableColumns[index].Name());
                        }
                    };

                    singeJoinParameters.KeySize = joinClause->ForeignEquations.size();
                    singeJoinParameters.IsLeft = joinClause->IsLeft;
                    singeJoinParameters.IsPartiallySorted = joinClause->ForeignKeyPrefix < foreignEquations.size();
                    singeJoinParameters.ForeignColumns = foreignColumns;
                    singeJoinParameters.ExecuteForeign = joinProfiler(subquery, joinClause);
                }
                joinParameters.Items.push_back(std::move(singeJoinParameters));

                lastSchema = joinClause->GetTableSchema(lastSchema);
            }

            std::sort(selfColumnNames.begin(), selfColumnNames.end());

            const auto& selfTableColumns = schema.Columns();

            std::vector<std::pair<size_t, EValueType>> primaryColumns;
            for (size_t index = 0; index < selfTableColumns.size(); ++index) {
                if (std::binary_search(
                    selfColumnNames.begin(),
                    selfColumnNames.end(),
                    selfTableColumns[index].Name()))
                {
                    primaryColumns.emplace_back(index, selfTableColumns[index].GetPhysicalType());

                    Fold(index);
                    Fold(static_cast<int>(selfTableColumns[index].GetPhysicalType()));
                }
            }

            joinParameters.PrimaryRowSize = primaryColumns.size();
            joinParameters.BatchSize = joinBatchSize;

            int index = Variables_->AddOpaque<TMultiJoinParameters>(joinParameters);

            Fold(index);

            auto fragmentInfos = equationFragments.ToFragmentInfos("selfEquation");
            for (const auto& codegenParameters: parameters) {
                equationFragments.DumpArgs(codegenParameters.Equations);
            }

            currentSlot = MakeCodegenMultiJoinOp(
                codegenSource,
                slotCount,
                currentSlot,
                index,
                fragmentInfos,
                std::move(parameters),
                std::move(primaryColumns),
                ComparerManager_);

            MakeCodegenFragmentBodies(codegenSource, fragmentInfos);

            schema = lastSchema;
            TSchemaProfiler::Profile(schema);
        }
    } else {
        for (const auto& joinClause : query->JoinClauses) {
            Fold(static_cast<int>(EFoldingObjectType::JoinOp));

            TExpressionFragments selfEquationFragments;

            std::vector<std::pair<size_t, bool>> selfKeys;
            for (const auto& column : joinClause->SelfEquations) {
                TConstExpressionPtr expression;
                bool isEvaluated;
                std::tie(expression, isEvaluated) = column;

                const auto& expressionSchema = isEvaluated ? joinClause->Schema.Original : schema;

                selfKeys.emplace_back(
                    TExpressionProfiler::Profile(
                        expression,
                        expressionSchema,
                        &selfEquationFragments,
                        isEvaluated),
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
                joinBatchSize = query->Offset + query->Limit;
            }

            TJoinParameters joinParameters;
            {
                const auto& foreignEquations = joinClause->ForeignEquations;
                auto commonKeyPrefix = joinClause->CommonKeyPrefix;

                // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
                auto subquery = New<TQuery>();

                subquery->Schema.Original = joinClause->Schema.Original;
                subquery->Schema.Mapping = joinClause->Schema.Mapping;

                // (join key... , other columns...)
                auto projectClause = New<TProjectClause>();
                std::vector<TConstExpressionPtr> joinKeyExprs;

                for (const auto& column : foreignEquations) {
                    projectClause->AddProjection(column, InferName(column));
                }

                subquery->ProjectClause = projectClause;
                subquery->WhereClause = joinClause->Predicate;

                auto selfColumnNames = joinClause->SelfJoinedColumns;
                std::sort(selfColumnNames.begin(), selfColumnNames.end());

                const auto& selfTableColumns = schema.Columns();

                std::vector<size_t> selfColumns;
                for (size_t index = 0; index < selfTableColumns.size(); ++index) {
                    if (std::binary_search(
                        selfColumnNames.begin(),
                        selfColumnNames.end(),
                        selfTableColumns[index].Name()))
                    {
                        selfColumns.push_back(index);
                    }
                }

                auto foreignColumnNames = joinClause->ForeignJoinedColumns;
                std::sort(foreignColumnNames.begin(), foreignColumnNames.end());

                auto joinRenamedTableColumns = joinClause->GetRenamedSchema().Columns();

                std::vector<size_t> foreignColumns;
                for (size_t index = 0; index < joinRenamedTableColumns.size(); ++index) {
                    if (std::binary_search(
                        foreignColumnNames.begin(),
                        foreignColumnNames.end(),
                        joinRenamedTableColumns[index].Name()))
                    {
                        foreignColumns.push_back(projectClause->Projections.size());

                        projectClause->AddProjection(
                            New<TReferenceExpression>(
                                joinRenamedTableColumns[index].GetPhysicalType(),
                                joinRenamedTableColumns[index].Name()),
                            joinRenamedTableColumns[index].Name());
                    }
                };

                joinParameters.IsOrdered = query->IsOrdered();
                joinParameters.IsLeft = joinClause->IsLeft;
                joinParameters.SelfColumns = selfColumns;
                joinParameters.ForeignColumns = foreignColumns;
                joinParameters.IsSortMergeJoin = commonKeyPrefix > 0;
                joinParameters.CommonKeyPrefixDebug = commonKeyPrefix;
                joinParameters.IsPartiallySorted = joinClause->ForeignKeyPrefix < foreignEquations.size();
                joinParameters.BatchSize = joinBatchSize;
                joinParameters.ExecuteForeign = joinProfiler(subquery, joinClause);
                joinParameters.PrimaryRowSize = schema.GetColumnCount();
            }

            int index = Variables_->AddOpaque<TJoinParameters>(joinParameters);

            Fold(index);

            YT_VERIFY(joinClause->CommonKeyPrefix < 1000);

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
                joinClause->CommonKeyPrefix,
                joinClause->ForeignKeyPrefix,
                ComparerManager_);

            MakeCodegenFragmentBodies(codegenSource, fragmentInfos);

            schema = joinClause->GetTableSchema(schema);
            TSchemaProfiler::Profile(schema);
        }
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

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstFrontQueryPtr& query,
    size_t* slotCount)
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
    Fold(resultId);
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
    const TConstExpressionPtr& expr,
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
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler,
    bool useMultijoin,
    const TConstFunctionProfilerMapPtr& functionProfilers,
    const TConstAggregateProfilerMapPtr& aggregateProfilers)
{
    TQueryProfiler profiler(id, variables, functionProfilers, aggregateProfilers);

    size_t slotCount = 0;
    TCodegenSource codegenSource = &CodegenEmptyOp;

    if (auto derivedQuery = dynamic_cast<const TQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount, joinProfiler, useMultijoin);
    } else if (auto derivedQuery = dynamic_cast<const TFrontQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount);
    } else {
        YT_ABORT();
    }

    return [
            MOVE(codegenSource),
            slotCount
        ] () {
            return CodegenEvaluate(&codegenSource, slotCount);
        };
}

////////////////////////////////////////////////////////////////////////////////

TExtraColumnsChecker::TExtraColumnsChecker(const THashSet<TString>& names)
    : Names(names)
{ }

void TExtraColumnsChecker::OnReference(const TReferenceExpression* referenceExpr)
{
    HasExtraColumns |= Names.count(referenceExpr->ColumnName) == 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

