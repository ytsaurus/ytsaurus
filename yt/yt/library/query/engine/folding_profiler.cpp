#include "folding_profiler.h"
#include "cg_fragment_compiler.h"
#include "functions_cg.h"
#include "llvm_folding_set.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <library/cpp/yt/memory/shared_range.h>

namespace NYT::NQueryClient {
namespace {

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFoldingObjectType,
    (ScanOp)
    (SplitterOp)
    (JoinOp)
    (ArrayJoinOp)
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
    (CaseExpr)
    (LikeExpr)
    (CompositeMemberAccessorExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
    (FinalMode)
    (MergeMode)
    (TotalsMode)
);

//! Computes a strong structural hash used to cache query fragments.

////////////////////////////////////////////////////////////////////////////////

std::vector<EValueType> GetTypesFromSchema(const TTableSchema& tableSchema)
{
    std::vector<EValueType> result;
    result.reserve(tableSchema.GetColumnCount());

    for (const auto& column : tableSchema.Columns()) {
        result.push_back(column.GetWireType());
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

    void Profile(const TTableSchemaPtr& tableSchema);

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

void TSchemaProfiler::Profile(const TTableSchemaPtr& tableSchema)
{
    const auto& columns = tableSchema->Columns();
    Fold(static_cast<int>(EFoldingObjectType::TableSchema));
    for (int index = 0; index < std::ssize(columns); ++index) {
        const auto& column = columns[index];
        Fold(static_cast<ui8>(column.GetWireType()));
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
    std::optional<size_t> ExtraArg2;

    TDebugInfo(
        const TConstExpressionPtr& expr,
        const std::vector<size_t>& args,
        const std::optional<size_t>& extraArg = std::nullopt,
        const std::optional<size_t>& extraArg2 = std::nullopt)
        : Expr(expr)
        , Args(args)
        , ExtraArg(extraArg)
        , ExtraArg2(extraArg2)
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

    void OnOperand(const TUnaryOpExpression* /*unaryExpr*/, size_t id)
    {
        auto operandId = DebugExpressions[id].Args[0];
        InferNameArg(operandId);
    }

    void OnLhs(const TBinaryOpExpression* /*binaryExpr*/, size_t id)
    {
        auto lhsId = DebugExpressions[id].Args[0];
        InferNameArg(lhsId);
    }

    void OnRhs(const TBinaryOpExpression* /*binaryExpr*/, size_t id)
    {
        auto rhsId = DebugExpressions[id].Args[1];
        InferNameArg(rhsId);
    }

    void OnDefaultExpression(const TTransformExpression* /*transformExpr*/, size_t id)
    {
        const auto& defaultExpr = DebugExpressions[id].ExtraArg;
        if (defaultExpr) {
            Builder->AppendString(", ");
            InferNameArg(*defaultExpr);
        }
    }

    void OnOptionalOperand(const TCaseExpression* /*caseExpr*/, size_t id)
    {
        const auto& optionalOperand = DebugExpressions[id].ExtraArg2;
        if (optionalOperand) {
            Builder->AppendChar(' ');
            InferNameArg(*optionalOperand);
        }
    }

    void OnWhenThenExpressions(const TCaseExpression* /*caseExpr*/, size_t id)
    {
        YT_ASSERT(DebugExpressions[id].Args.size() % 2 == 0);

        auto whenThenExpressionsLength = DebugExpressions[id].Args.size() / 2;
        auto conditions = MakeRange(DebugExpressions[id].Args.data(), whenThenExpressionsLength);
        auto results = MakeRange(DebugExpressions[id].Args.data() + whenThenExpressionsLength, whenThenExpressionsLength);

        for (size_t index = 0; index < whenThenExpressionsLength; ++index) {
            Builder->AppendString(" WHEN ");
            InferNameArg(conditions[index]);
            Builder->AppendString(" THEN ");
            InferNameArg(results[index]);
        }
    }

    void OnDefaultExpression(const TCaseExpression* /*caseExpr*/, size_t id)
    {
        const auto& defaultExpr = DebugExpressions[id].ExtraArg;
        if (defaultExpr) {
            Builder->AppendString(", ");
            InferNameArg(*defaultExpr);
        }
    }

    void OnLikeText(const TLikeExpression* /*likeExpr*/, size_t id)
    {
        auto textId = DebugExpressions[id].Args[0];
        InferNameArg(textId);
    }

    void OnLikePattern(const TLikeExpression* /*likeExpr*/, size_t id)
    {
        auto patternId = DebugExpressions[id].Args[1];
        InferNameArg(patternId);
    }

    void OnLikeEscapeCharacter(const TLikeExpression* /*likeExpr*/, size_t id)
    {
        auto escapeCharacterId = *DebugExpressions[id].ExtraArg;
        InferNameArg(escapeCharacterId);
    }

    void OnCompositeMemberAccessorColumnReference(const TCompositeMemberAccessorExpression* /*memberAccessorExpr*/, size_t id)
    {
        auto referenceId = DebugExpressions[id].Args[0];
        InferNameArg(referenceId);
    }

    template <class T>
    void OnArguments(const T* /*expr*/, size_t id)
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

    void OnReference(const TReferenceExpression* referenceExpr, size_t /*id*/)
    {
        auto columnName = referenceExpr->ColumnName;
        if (columnName.size() > 40) {
            columnName.resize(40);
            columnName.Transform([] (size_t /*index*/, char c) {
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
        const TConstFunctionProfilerMapPtr& functionProfilers,
        bool useCanonicalNullRelations,
        EExecutionBackend executionBackend)
        : TSchemaProfiler(id)
        , Variables_(variables)
        , FunctionProfilers_(functionProfilers)
        , ComparerManager_(MakeComparerManager())
        , UseCanonicalNullRelations_(useCanonicalNullRelations)
        , ExecutionBackend_(executionBackend)
    {
        YT_VERIFY(Variables_);
    }

    size_t Profile(
        const TConstExpressionPtr& expr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated = false);

private:
    size_t Profile(
        const TLiteralExpression* literalExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TReferenceExpression* referenceExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TFunctionExpression* functionExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TUnaryOpExpression* unaryOp,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TBinaryOpExpression* binaryOp,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TInExpression* inExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TBetweenExpression* betweenExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TTransformExpression* transformExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TCaseExpression* caseExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TLikeExpression* likeExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

    size_t Profile(
        const TCompositeMemberAccessorExpression* memberAccessorExpr,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments,
        bool isolated);

protected:
    TCGVariables* const Variables_;
    const TConstFunctionProfilerMapPtr FunctionProfilers_;
    const TComparerManagerPtr ComparerManager_;
    const bool UseCanonicalNullRelations_;
    const EExecutionBackend ExecutionBackend_;
};

size_t* TryGetSubexpressionRef(
    TExpressionFragments* fragments,
    const llvm::FoldingSetNodeID& id,
    bool isolated)
{
    auto result = fragments->Items.size();
    if (!isolated) {
        auto [it, emplaced] = fragments->Fingerprints.emplace(id, result);
        if (!emplaced) {
            return &it->second;
        }
    }
    return nullptr;
}

size_t TExpressionProfiler::Profile(
    const TLiteralExpression* literalExpr,
    const TTableSchemaPtr& /*schema*/,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::LiteralExpr));
    id.AddInteger(static_cast<ui8>(literalExpr->GetWireType()));

    auto savedId = id;
    id.AddString(ToString(TValue(literalExpr->Value)).c_str());

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);

    int index = Variables_->AddLiteralValue(literalExpr->Value);
    Fold(index);

    bool nullable = TValue(literalExpr->Value).Type == EValueType::Null;
    Fold(nullable);

    fragments->DebugInfos.emplace_back(literalExpr, std::vector<size_t>());
    fragments->Items.emplace_back(
        MakeCodegenLiteralExpr(index, nullable, literalExpr->GetWireType()),
        literalExpr->GetWireType(),
        nullable,
        true);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TReferenceExpression* referenceExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::ReferenceExpr));
    id.AddInteger(static_cast<ui8>(referenceExpr->GetWireType()));

    auto indexInSchema = schema->GetColumnIndexOrThrow(referenceExpr->ColumnName);
    id.AddInteger(indexInSchema);

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(id);
    fragments->DebugInfos.emplace_back(referenceExpr, std::vector<size_t>());
    fragments->Items.emplace_back(
        MakeCodegenReferenceExpr(
            indexInSchema,
            referenceExpr->GetWireType(),
            referenceExpr->ColumnName),
        referenceExpr->GetWireType(),
        true,
        true);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TFunctionExpression* functionExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::FunctionExpr));
    id.AddInteger(static_cast<ui8>(functionExpr->GetWireType()));
    id.AddString(functionExpr->FunctionName.c_str());

    int argumentCount = functionExpr->Arguments.size();
    std::vector<size_t> argIds(argumentCount);
    std::vector<EValueType> argumentTypes(argumentCount);
    auto literalArgs = std::make_unique<bool[]>(argumentCount);
    id.AddInteger(argumentCount);
    for (int index = 0; index < argumentCount; ++index) {
        const auto& argument = functionExpr->Arguments[index];
        // If fold is called inside Profile(argument) than in current function Fold will be called too.
        argIds[index] = Profile(argument, schema, fragments, isolated);
        id.AddInteger(argIds[index]);
        argumentTypes[index] = argument->GetWireType();
        literalArgs[index] = argument->As<TLiteralExpression>() != nullptr;
    }

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

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
            functionExpr->GetWireType(),
            "{" + InferName(functionExpr, true) + "}",
            ExecutionBackend_,
            Id_),
        functionExpr->GetWireType(),
        function->IsNullable(nullableArgs));
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TUnaryOpExpression* unaryOp,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::UnaryOpExpr));
    id.AddInteger(static_cast<ui8>(unaryOp->GetWireType()));
    id.AddInteger(static_cast<int>(unaryOp->Opcode));

    size_t operand = Profile(unaryOp->Operand, schema, fragments, isolated);
    id.AddInteger(operand);

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(id);
    ++fragments->Items[operand].UseCount;
    fragments->DebugInfos.emplace_back(unaryOp, std::vector<size_t>{operand});
    fragments->Items.emplace_back(MakeCodegenUnaryOpExpr(
        unaryOp->Opcode,
        operand,
        unaryOp->GetWireType(),
        "{" + InferName(unaryOp, true) + "}"),
        unaryOp->GetWireType(),
        fragments->Items[operand].Nullable);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TBinaryOpExpression* binaryOp,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
    id.AddInteger(static_cast<ui8>(binaryOp->GetWireType()));
    id.AddInteger(static_cast<int>(binaryOp->Opcode));
    id.AddInteger(UseCanonicalNullRelations_);

    size_t lhsOperand = Profile(binaryOp->Lhs, schema, fragments, isolated);
    id.AddInteger(lhsOperand);
    size_t rhsOperand = Profile(binaryOp->Rhs, schema, fragments, isolated);
    id.AddInteger(rhsOperand);

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(id);
    ++fragments->Items[lhsOperand].UseCount;
    ++fragments->Items[rhsOperand].UseCount;
    fragments->DebugInfos.emplace_back(binaryOp, std::vector<size_t>{lhsOperand, rhsOperand});
    bool nullable = IsRelationalBinaryOp(binaryOp->Opcode) && !UseCanonicalNullRelations_
        ? false
        : fragments->Items[lhsOperand].Nullable || fragments->Items[rhsOperand].Nullable;
    fragments->Items.emplace_back(MakeCodegenBinaryOpExpr(
        binaryOp->Opcode,
        lhsOperand,
        rhsOperand,
        binaryOp->GetWireType(),
        "{" + InferName(binaryOp, true) + "}",
        UseCanonicalNullRelations_),
        binaryOp->GetWireType(),
        nullable);
    return fragments->Items.size() - 1;
}

////////////////////////////////////////////////////////////////////////////////

size_t TExpressionProfiler::Profile(
    const TInExpression* inExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::InExpr));
    id.AddInteger(static_cast<ui8>(inExpr->GetWireType()));

    std::vector<size_t> argIds;
    argIds.reserve(inExpr->Arguments.size());
    id.AddInteger(inExpr->Arguments.size());
    for (const auto& argument : inExpr->Arguments) {
        // If fold is called inside Profile(argument) than in current function Fold will be called too.
        argIds.push_back(Profile(argument, schema, fragments, isolated));
        id.AddInteger(argIds.back());
    }

    auto savedId = id;
    for (const auto& value : inExpr->Values) {
        id.AddString(ToString(value).c_str());
    }

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);
    for (size_t argId : argIds) {
        ++fragments->Items[argId].UseCount;
    }

    int index = Variables_->AddOpaque<TSharedRange<TRange<TPIValue>>>(CopyAndConvertToPI(inExpr->Values, false));
    int hashtableIndex = Variables_->AddOpaque<std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>>();
    fragments->DebugInfos.emplace_back(inExpr, argIds);
    fragments->Items.emplace_back(
        MakeCodegenInExpr(std::move(argIds), index, hashtableIndex, ComparerManager_),
        inExpr->GetWireType(),
        false);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TBetweenExpression* betweenExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::BetweenExpr));
    id.AddInteger(static_cast<ui8>(betweenExpr->GetWireType()));

    std::vector<size_t> argIds;
    argIds.reserve(betweenExpr->Arguments.size());
    id.AddInteger(betweenExpr->Arguments.size());
    for (const auto& argument : betweenExpr->Arguments) {
        // If fold is called inside Profile(argument) than in current function Fold will be called too.
        argIds.push_back(Profile(argument, schema, fragments, isolated));
        id.AddInteger(argIds.back());
    }

    auto savedId = id;
    for (const auto& range : betweenExpr->Ranges) {
        id.AddString(ToString(range.first).c_str());
        id.AddString(ToString(range.second).c_str());
    }

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);
    for (size_t argId : argIds) {
        ++fragments->Items[argId].UseCount;
    }

    int index = Variables_->AddOpaque<TSharedRange<TPIRowRange>>(CopyAndConvertToPI(betweenExpr->Ranges, false));
    fragments->DebugInfos.emplace_back(betweenExpr, argIds);
    fragments->Items.emplace_back(
        MakeCodegenBetweenExpr(std::move(argIds), index, ComparerManager_),
        betweenExpr->GetWireType(),
        false);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TTransformExpression* transformExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::TransformExpr));
    id.AddInteger(static_cast<ui8>(transformExpr->GetWireType()));

    const auto& defaultExpression = transformExpr->DefaultExpression;

    std::vector<size_t> argIds;
    argIds.reserve(transformExpr->Arguments.size());
    id.AddInteger(transformExpr->Arguments.size() + static_cast<bool>(defaultExpression));
    for (const auto& argument : transformExpr->Arguments) {
        // If fold is called inside Profile(argument) than in current function Fold will be called too.
        argIds.push_back(Profile(argument, schema, fragments, isolated));
        id.AddInteger(argIds.back());
    }

    std::optional<size_t> defaultExprId;
    if (defaultExpression) {
        defaultExprId = Profile(defaultExpression, schema, fragments, isolated);
        id.AddInteger(*defaultExprId);
    }

    auto savedId = id;
    for (const auto& value : transformExpr->Values) {
        id.AddString(ToString(value).c_str());
    }

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

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

    int index = Variables_->AddOpaque<TSharedRange<TRange<TPIValue>>>(CopyAndConvertToPI(transformExpr->Values, false));
    int hashtableIndex = Variables_->AddOpaque<std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>>();

    fragments->DebugInfos.emplace_back(transformExpr, argIds, defaultExprId);
    fragments->Items.emplace_back(
        MakeCodegenTransformExpr(std::move(argIds), defaultExprId, index, hashtableIndex, transformExpr->GetWireType(), ComparerManager_),
        transformExpr->GetWireType(),
        nullable);
    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TCaseExpression* caseExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::CaseExpr));
    id.AddInteger(static_cast<ui8>(caseExpr->GetWireType()));
    id.AddBoolean(static_cast<bool>(caseExpr->OptionalOperand));
    if (caseExpr->OptionalOperand) {
        id.AddInteger(static_cast<ui8>(caseExpr->OptionalOperand->GetWireType()));
    } else {
        id.AddInteger(static_cast<ui8>(EValueType::Min));
    }
    id.AddInteger(caseExpr->WhenThenExpressions.size());
    id.AddBoolean(static_cast<bool>(caseExpr->DefaultExpression));

    std::optional<size_t> optionalOperandId;
    if (caseExpr->OptionalOperand) {
        optionalOperandId = Profile(caseExpr->OptionalOperand, schema, fragments, isolated);
        id.AddInteger(*optionalOperandId);
    }

    std::vector<std::pair<size_t, size_t>> whenThenExpressionIds;
    whenThenExpressionIds.reserve(caseExpr->WhenThenExpressions.size());
    for (const auto& caseClause : caseExpr->WhenThenExpressions) {
        size_t conditionId = Profile(caseClause->Condition, schema, fragments, isolated);
        size_t resultId = Profile(caseClause->Result, schema, fragments, isolated);

        whenThenExpressionIds.emplace_back(conditionId, resultId);

        id.AddInteger(conditionId);
        id.AddInteger(resultId);
    }

    std::optional<size_t> defaultExprId;
    if (caseExpr->DefaultExpression) {
        defaultExprId = Profile(caseExpr->DefaultExpression, schema, fragments, isolated);
        id.AddInteger(*defaultExprId);
    }

    auto savedId = id;

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);
    if (optionalOperandId) {
        ++fragments->Items[*optionalOperandId].UseCount;
    }

    for (auto& [conditionId, resultId] : whenThenExpressionIds) {
        ++fragments->Items[conditionId].UseCount;
        ++fragments->Items[resultId].UseCount;
    }

    bool nullable = true;

    if (defaultExprId) {
        ++fragments->Items[*defaultExprId].UseCount;

        nullable = false;
        nullable |= fragments->Items[*defaultExprId].Nullable;
    }

    auto argIds = std::vector<size_t>();
    argIds.reserve(caseExpr->WhenThenExpressions.size() * 2);
    for (auto& [conditionId, _] : whenThenExpressionIds) {
        argIds.push_back(conditionId);
    }
    for (auto& [_, resultId] : whenThenExpressionIds) {
        argIds.push_back(resultId);
    }
    fragments->DebugInfos.emplace_back(caseExpr, argIds, defaultExprId, optionalOperandId);

    std::optional<EValueType> optionalOperandType;
    if (caseExpr->OptionalOperand) {
        optionalOperandType = caseExpr->OptionalOperand->GetWireType();
    }

    fragments->Items.emplace_back(
        MakeCodegenCaseExpr(
            optionalOperandId,
            optionalOperandType,
            std::move(whenThenExpressionIds),
            defaultExprId,
            caseExpr->GetWireType(),
            ComparerManager_),
        caseExpr->GetWireType(),
        nullable);

    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TLikeExpression* likeExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(ExecutionBackend_));
    id.AddInteger(static_cast<int>(EFoldingObjectType::LikeExpr));

    YT_VERIFY(likeExpr->GetWireType() == EValueType::Boolean);

    auto textId = Profile(likeExpr->Text, schema, fragments, isolated);
    id.AddInteger(textId);

    id.AddInteger(static_cast<int>(likeExpr->Opcode));

    auto patternId = Profile(likeExpr->Pattern, schema, fragments, isolated);
    id.AddInteger(patternId);

    std::optional<size_t> escapeCharacterId;
    if (likeExpr->EscapeCharacter) {
        YT_VERIFY(likeExpr->Opcode != EStringMatchOp::Regex);
        escapeCharacterId = Profile(likeExpr->EscapeCharacter, schema, fragments, isolated);
        id.AddInteger(*escapeCharacterId);
    }

    auto savedId = id;

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);

    ++fragments->Items[textId].UseCount;
    ++fragments->Items[patternId].UseCount;
    if (escapeCharacterId) {
        ++fragments->Items[*escapeCharacterId].UseCount;
    }

    bool nullable = true;
    nullable |= fragments->Items[textId].Nullable;
    nullable |= fragments->Items[patternId].Nullable;
    if (escapeCharacterId) {
        nullable |= fragments->Items[*escapeCharacterId].Nullable;
    }

    if (!escapeCharacterId) {
        fragments->DebugInfos.emplace_back(
            likeExpr,
            std::vector{textId, patternId});
    } else {
        fragments->DebugInfos.emplace_back(
            likeExpr,
            std::vector{textId, patternId},
            *escapeCharacterId);
    }

    int opaqueIndex = -1;
    {
        auto patternLiteral = likeExpr->Pattern->As<TLiteralExpression>();
        bool patternIsLiteral = patternLiteral != nullptr;
        auto patternValue = MakeUnversionedNullValue();
        if (patternIsLiteral) {
            patternValue = patternLiteral->Value;
            YT_ASSERT(patternValue.Type == EValueType::String || patternValue.Type == EValueType::Null);
        }

        bool haveEscape = likeExpr->EscapeCharacter != nullptr;
        const TLiteralExpression* escapeLiteral = haveEscape ? likeExpr->EscapeCharacter->As<TLiteralExpression>() : nullptr;
        bool escapeIsLiteral = escapeLiteral != nullptr;
        auto escapeValue = MakeUnversionedNullValue();
        if (escapeIsLiteral) {
            escapeValue = escapeLiteral->Value;
            YT_ASSERT(escapeValue.Type == EValueType::String || escapeValue.Type == EValueType::Null);
        }

        std::unique_ptr<re2::RE2> precompiledRegex;
        if (patternIsLiteral && (!haveEscape || escapeIsLiteral)) {
            auto re2Pattern = ConvertLikePatternToRegex(
                patternValue.AsStringBuf(),
                likeExpr->Opcode,
                haveEscape ? escapeValue.AsStringBuf() : TStringBuf(),
                haveEscape);

            re2::RE2::Options options;
            options.set_log_errors(false);

            precompiledRegex = std::make_unique<re2::RE2>(re2::StringPiece(re2Pattern.Data(), re2Pattern.Size()), options);
        }

        opaqueIndex = Variables_->AddOpaque<TLikeExpressionContext>(std::move(precompiledRegex));
    }

    fragments->Items.emplace_back(
        MakeCodegenLikeExpr(
            textId,
            likeExpr->Opcode,
            patternId,
            escapeCharacterId,
            opaqueIndex),
        likeExpr->GetWireType(),
        nullable);

    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TCompositeMemberAccessorExpression* memberAccessorExpr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    llvm::FoldingSetNodeID id;
    id.AddInteger(static_cast<int>(EFoldingObjectType::CompositeMemberAccessorExpr));

    auto compositeId = Profile(memberAccessorExpr->CompositeExpression, schema, fragments, isolated);
    id.AddInteger(compositeId);

    id.AddInteger(std::ssize(memberAccessorExpr->NestedStructOrTupleItemAccessor.NestedTypes));
    for (int index = 0; index < std::ssize(memberAccessorExpr->NestedStructOrTupleItemAccessor.NestedTypes); ++index) {
        id.AddInteger(static_cast<int>(memberAccessorExpr->NestedStructOrTupleItemAccessor.NestedTypes[index]));
        id.AddString(memberAccessorExpr->NestedStructOrTupleItemAccessor.NamedStructMembers[index].c_str());
        id.AddInteger(memberAccessorExpr->NestedStructOrTupleItemAccessor.PositionalStructMembers[index]);
        id.AddInteger(memberAccessorExpr->NestedStructOrTupleItemAccessor.TupleItemIndices[index]);
    }

    id.AddBoolean(static_cast<bool>(memberAccessorExpr->DictOrListItemAccessor));
    size_t dictOrListItemAccessorId = 0;
    if (memberAccessorExpr->DictOrListItemAccessor) {
        dictOrListItemAccessorId = Profile(memberAccessorExpr->DictOrListItemAccessor, schema, fragments, isolated);
        id.AddInteger(dictOrListItemAccessorId);
    }

    auto savedId = id;

    if (const auto* ref = TryGetSubexpressionRef(fragments, id, isolated)) {
        return *ref;
    }

    Fold(savedId);

    ++fragments->Items[compositeId].UseCount;

    if (memberAccessorExpr->DictOrListItemAccessor) {
        ++fragments->Items[dictOrListItemAccessorId].UseCount;
    }

    bool nullable = true;

    fragments->DebugInfos.emplace_back(memberAccessorExpr, std::vector{compositeId});

    int opaqueIndex = Variables_->AddOpaque<TCompositeMemberAccessorPath>(memberAccessorExpr->NestedStructOrTupleItemAccessor);

    fragments->Items.emplace_back(
        MakeCodegenCompositeMemberAccessorExpr(
            compositeId,
            opaqueIndex,
            memberAccessorExpr->DictOrListItemAccessor ? std::optional<size_t>(dictOrListItemAccessorId) : std::nullopt,
            memberAccessorExpr->GetWireType()),
        memberAccessorExpr->GetWireType(),
        nullable);

    return fragments->Items.size() - 1;
}

size_t TExpressionProfiler::Profile(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments,
    bool isolated)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return Profile(literalExpr, schema, fragments, isolated);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return Profile(referenceExpr, schema, fragments, isolated);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        return Profile(functionExpr, schema, fragments, isolated);
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        return Profile(unaryOp, schema, fragments, isolated);
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        return Profile(binaryOp, schema, fragments, isolated);
    } else if (auto inExpr = expr->As<TInExpression>()) {
        return Profile(inExpr, schema, fragments, isolated);
    } else if (auto betweenExpr = expr->As<TBetweenExpression>()) {
        return Profile(betweenExpr, schema, fragments, isolated);
    } else if (auto transformExpr = expr->As<TTransformExpression>()) {
        return Profile(transformExpr, schema, fragments, isolated);
    } else if (auto caseExpr = expr->As<TCaseExpression>()) {
        return Profile(caseExpr, schema, fragments, isolated);
    } else if (auto likeExpr = expr->As<TLikeExpression>()) {
        return Profile(likeExpr, schema, fragments, isolated);
    } else if (auto memberAccessorExpr = expr->As<TCompositeMemberAccessorExpression>()) {
        return Profile(memberAccessorExpr, schema, fragments, isolated);
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
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        bool useCanonicalNullRelations,
        EExecutionBackend executionBackend)
        : TExpressionProfiler(id, variables, functionProfilers, useCanonicalNullRelations, executionBackend)
        , AggregateProfilers_(aggregateProfilers)
    { }

    void Profile(
        TCodegenSource* codegenSource,
        const TConstBaseQueryPtr& query,
        size_t* slotCount,
        size_t inputSlot,
        TTableSchemaPtr schema,
        bool mergeMode);

    void Profile(
        TCodegenSource* codegenSource,
        const TConstQueryPtr& query,
        size_t* slotCount,
        TJoinSubqueryProfiler joinProfiler);

    void Profile(
        TCodegenSource* codegenSource,
        const TConstFrontQueryPtr& query,
        size_t* slotCount);

protected:
    const TConstAggregateProfilerMapPtr AggregateProfilers_;

    size_t Profile(
        const TNamedItem& namedExpression,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments);
};

////////////////////////////////////////////////////////////////////////////////

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstBaseQueryPtr& query,
    size_t* slotCount,
    size_t inputSlot,
    TTableSchemaPtr schema,
    bool mergeMode)
{
    Fold(static_cast<int>(ExecutionBackend_));
    size_t dummySlot = (*slotCount)++;

    size_t finalSlot = dummySlot;
    size_t intermediateSlot = dummySlot;
    size_t totalsSlot = dummySlot;

    bool finalMode = query->IsFinal;

    Fold(static_cast<int>(EFoldingObjectType::FinalMode));
    Fold(static_cast<int>(finalMode));
    Fold(static_cast<int>(EFoldingObjectType::MergeMode));
    Fold(static_cast<int>(mergeMode));

    if (auto groupClause = query->GroupClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::GroupOp));
        Fold(static_cast<int>(groupClause->CommonPrefixWithPrimaryKey));
        Fold(static_cast<int>(query->UseDisjointGroupBy));

        std::vector<EValueType> keyTypes;
        std::vector<EValueType> stateTypes;

        std::vector<size_t> groupExprIds;
        std::vector<std::vector<size_t>> aggregateExprIdsByFunc;
        std::vector<TCodegenAggregate> codegenAggregates;

        TExpressionFragments expressionFragments;

        TExpressionFragments groupFragments;
        for (const auto& groupItem : groupClause->GroupItems) {
            size_t groupExprId;
            if (mergeMode) {
                auto referenceExpr = New<TReferenceExpression>(
                    groupItem.Expression->LogicalType,
                    groupItem.Name);
                groupExprId = Profile(TNamedItem(std::move(referenceExpr), groupItem.Name), schema, &expressionFragments);
            } else {
                groupExprId = Profile(groupItem, schema, &expressionFragments);
            }
            groupExprIds.push_back(groupExprId);

            keyTypes.push_back(groupItem.Expression->GetWireType());
        }

        bool allAggregatesFirst = true;
        for (const auto& aggregateItem : groupClause->AggregateItems) {
            Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
            Fold(aggregateItem.AggregateFunction.c_str());
            Fold(aggregateItem.Name.c_str());

            const auto& aggregate = AggregateProfilers_->GetAggregate(aggregateItem.AggregateFunction);

            if (!aggregate->IsFirst()) {
                allAggregatesFirst = false;
            }

            int argumentCount = aggregateItem.Arguments.size();

            if (!mergeMode) {
                std::vector<size_t> aggregateArgIds;
                aggregateArgIds.reserve(argumentCount);
                for (auto& arg : aggregateItem.Arguments) {
                    aggregateArgIds.push_back(Profile(TNamedItem{arg, ""}, schema, &expressionFragments));
                }
                aggregateExprIdsByFunc.emplace_back(std::move(aggregateArgIds));
            }
            std::vector<EValueType> wireTypes;
            wireTypes.reserve(argumentCount);
            for (const auto& arg : aggregateItem.Arguments) {
                wireTypes.push_back(arg->GetWireType());
            }
            codegenAggregates.push_back(aggregate->Profile(
                std::move(wireTypes),
                aggregateItem.StateType,
                aggregateItem.ResultType,
                aggregateItem.Name,
                ExecutionBackend_,
                Id_));
            stateTypes.push_back(aggregateItem.StateType);
        }

        auto fragmentInfos = expressionFragments.ToFragmentInfos("groupExpression");
        for (const auto& funcArgs : aggregateExprIdsByFunc) {
            expressionFragments.DumpArgs(funcArgs);
        }
        expressionFragments.DumpArgs(groupExprIds);

        // If group key contains primary key prefix, full grouped rowset is not kept till the end but flushed
        // every time prefix changes (scan is ordered by primary key, bottom queries are always evaluated along
        // each tablet). Grouped rows with inner primary key prefix are transferred to final slot (they are
        // disjoint). Grouped rows with boundary primary key prefix (with respect to tablet) are transferred to
        // intermediate slot and need final grouping.

        auto fragmentSlots = MakeCodegenGroupOp(
            codegenSource,
            slotCount,
            inputSlot,
            fragmentInfos,
            std::move(groupExprIds),
            std::move(aggregateExprIdsByFunc),
            codegenAggregates,
            keyTypes,
            stateTypes,
            allAggregatesFirst,
            mergeMode,
            groupClause->TotalsMode != ETotalsMode::None,
            // Input is ordered for ordered queries and bottom fragments if CommonPrefixWithPrimaryKey > 0.
            // Prefix comparer can be used only if input is ordered.
            !mergeMode || query->IsOrdered() ? groupClause->CommonPrefixWithPrimaryKey : 0,
            ComparerManager_);

        intermediateSlot = fragmentSlots.Intermediate;
        finalSlot = fragmentSlots.Final;
        size_t deltaFinalSlot = fragmentSlots.DeltaFinal;
        totalsSlot = fragmentSlots.Totals;

        Fold(static_cast<int>(EFoldingObjectType::TotalsMode));
        Fold(static_cast<int>(groupClause->TotalsMode));

        schema = groupClause->GetTableSchema(query->IsFinal);

        TCodegenFragmentInfosPtr havingFragmentsInfos;

        size_t havingPredicateId = 0;
        bool addHaving = query->HavingClause && !IsTrue(query->HavingClause);

        if (addHaving) {
            TExpressionFragments havingExprFragments;
            havingPredicateId = TExpressionProfiler::Profile(query->HavingClause, schema, &havingExprFragments);

            havingFragmentsInfos = havingExprFragments.ToFragmentInfos("havingExpression");
            havingExprFragments.DumpArgs(std::vector<size_t>{havingPredicateId});
        }

        // COMPAT(lukyan)
        if (finalMode || query->UseDisjointGroupBy) {
            // Boundary segments are also final
            deltaFinalSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                deltaFinalSlot);

            intermediateSlot = dummySlot;
        } else if (mergeMode) {
            // TODO(dtorilov): This relates to Node query level; consider removing this merge operation.
            intermediateSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                deltaFinalSlot);

            deltaFinalSlot = dummySlot;
        }

        size_t keySize = groupClause->GroupItems.size();

        if (!mergeMode || finalMode) {
            if (addHaving && groupClause->TotalsMode == ETotalsMode::AfterHaving) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));

                // Finalizes row to evaluate predicate and filters source values.
                deltaFinalSlot = MakeCodegenFilterFinalizedOp(
                    codegenSource,
                    slotCount,
                    deltaFinalSlot,
                    havingFragmentsInfos,
                    havingPredicateId,
                    keySize,
                    codegenAggregates,
                    stateTypes);
            }

            if (groupClause->TotalsMode != ETotalsMode::None) {
                size_t totalsSlotNew;
                std::tie(totalsSlotNew, deltaFinalSlot) = MakeCodegenDuplicateOp(
                    codegenSource,
                    slotCount,
                    deltaFinalSlot);

                if (mergeMode) {
                    totalsSlot = MakeCodegenMergeOp(
                        codegenSource,
                        slotCount,
                        totalsSlot,
                        totalsSlotNew);
                } else {
                    totalsSlot = totalsSlotNew;
                }
            }

            deltaFinalSlot = MakeCodegenFinalizeOp(
                codegenSource,
                slotCount,
                deltaFinalSlot,
                keySize,
                codegenAggregates,
                stateTypes);

            if (addHaving && groupClause->TotalsMode != ETotalsMode::AfterHaving) {
                Fold(static_cast<int>(EFoldingObjectType::HavingOp));
                deltaFinalSlot = MakeCodegenFilterOp(
                    codegenSource,
                    slotCount,
                    deltaFinalSlot,
                    havingFragmentsInfos,
                    havingPredicateId);
            }

            if (mergeMode) {
                finalSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    deltaFinalSlot,
                    finalSlot);
            } else {
                finalSlot = deltaFinalSlot;
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

            if (finalMode) {
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
        intermediateSlot = inputSlot;
        finalSlot = MakeCodegenMergeOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            finalSlot);
        intermediateSlot = dummySlot;
    }

    intermediateSlot = MakeCodegenOnceOp(codegenSource, slotCount, intermediateSlot);
    finalSlot = MakeCodegenOnceOp(codegenSource, slotCount, finalSlot);
    totalsSlot = MakeCodegenOnceOp(codegenSource, slotCount, totalsSlot);

    if (auto orderClause = query->OrderClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::OrderOp));

        int orderItemCount = orderClause->OrderItems.size();
        std::vector<size_t> orderExprIds(orderItemCount);
        std::vector<bool> isDesc(orderItemCount);
        std::vector<EValueType> orderColumnTypes(orderItemCount);
        TExpressionFragments orderExprFragments;
        for (int index = 0; index < orderItemCount; ++index) {
            const auto& item = orderClause->OrderItems[index];
            orderExprIds[index] = TExpressionProfiler::Profile(item.Expression, schema, &orderExprFragments);
            Fold(item.Descending);
            isDesc[index] = item.Descending;
            orderColumnTypes[index] = item.Expression->GetWireType();
        }

        auto orderFragmentsInfos = orderExprFragments.ToFragmentInfos("orderExpression");
        orderExprFragments.DumpArgs(orderExprIds);

        auto schemaTypes = GetTypesFromSchema(*schema);
        for (auto type : schemaTypes) {
            Fold(static_cast<ui8>(type));
        }

        finalSlot = MakeCodegenOrderOp(
            codegenSource,
            slotCount,
            finalSlot,
            orderFragmentsInfos,
            std::move(orderExprIds),
            std::move(orderColumnTypes),
            schemaTypes,
            std::move(isDesc),
            ComparerManager_);
        MakeCodegenFragmentBodies(codegenSource, orderFragmentsInfos);
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        std::vector<size_t> projectExprIds;
        projectExprIds.reserve(projectClause->Projections.size());
        TExpressionFragments projectExprFragments;
        for (const auto& item : projectClause->Projections) {
            projectExprIds.push_back(Profile(item, schema, &projectExprFragments));
        }

        auto projectFragmentsInfos = projectExprFragments.ToFragmentInfos("projectExpression");
        projectExprFragments.DumpArgs(projectExprIds);

        // FIXME(lukyan): Do not generate ProjectOp two times.
        finalSlot = MakeCodegenProjectOp(codegenSource, slotCount, finalSlot, projectFragmentsInfos, projectExprIds);
        totalsSlot = MakeCodegenProjectOp(codegenSource, slotCount, totalsSlot, projectFragmentsInfos, std::move(projectExprIds));

        MakeCodegenFragmentBodies(codegenSource, projectFragmentsInfos);

        schema = projectClause->GetTableSchema();
    }

    bool considerLimit = query->IsOrdered() && !query->GroupClause;
    Fold(static_cast<int>(considerLimit));
    if (considerLimit) {
        // TODO(dtorilov): Since we have already applied filters to queries with `having` at this stage,
        // it is safe to apply `limit` to the finalSlot of grouping queries.

        int offsetId = Variables_->AddOpaque<size_t>(query->Offset);
        int limitId = Variables_->AddOpaque<size_t>(query->Limit);

        finalSlot = MakeCodegenOffsetLimiterOp(codegenSource, slotCount, finalSlot, offsetId, limitId);
    }

    size_t resultRowSize = schema->GetColumnCount();

    if (!finalMode) {
        auto schemaTypes = GetTypesFromSchema(*schema);

        finalSlot = MakeCodegenAddStreamOp(
            codegenSource,
            slotCount,
            finalSlot,
            resultRowSize,
            EStreamTag::Final,
            schemaTypes);

        totalsSlot = MakeCodegenAddStreamOp(
            codegenSource,
            slotCount,
            totalsSlot,
            resultRowSize,
            EStreamTag::Totals,
            schemaTypes);

        intermediateSlot = MakeCodegenAddStreamOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            resultRowSize,
            EStreamTag::Intermediate,
            schemaTypes);

        ++resultRowSize;
    }

    size_t resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, finalSlot, totalsSlot);
    resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, resultSlot, intermediateSlot);

    //resultSlot = MakeCodegenOnceOp(codegenSource, slotCount, resultSlot);

    Fold(static_cast<int>(EFoldingObjectType::WriteOp));
    MakeCodegenWriteOp(codegenSource, resultSlot, resultRowSize);
}

std::vector<int> GetStringLikeColumnIndices(const TTableSchemaPtr& schema)
{
    std::vector<int> stringLikeColumnIndices;
    const auto& columns = schema->Columns();

    for (int index = 0; index < std::ssize(columns); ++index) {
        if (IsStringLikeType(columns[index].GetWireType())) {
            stringLikeColumnIndices.push_back(index);
        }
    }

    return stringLikeColumnIndices;
}

i64 InferRowWeightWithNoStrings(const TTableSchemaPtr& schema)
{
    i64 result = 1;

    const auto& columns = schema->Columns();
    for (int index = 0; index < std::ssize(columns); ++index) {
        auto wireType = columns[index].GetWireType();
        if (!IsStringLikeType(wireType)) {
            result += GetDataWeight(wireType);
        }
    }

    return result;
}

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstQueryPtr& query,
    size_t* slotCount,
    TJoinSubqueryProfiler joinProfiler)
{
    Fold(static_cast<int>(ExecutionBackend_));
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();
    TSchemaProfiler::Profile(schema);

    auto stringLikeColumnIndices = GetStringLikeColumnIndices(query->GetReadSchema());

    int rowSchemaInformationIndex = Variables_->AddOpaque<TRowSchemaInformation>(
        TRowSchemaInformation{
            InferRowWeightWithNoStrings(query->GetReadSchema()),
            stringLikeColumnIndices,
            query->GetReadSchema()->GetColumnCount() + 0}); // NB: This query has no incoming stream tag.

    size_t currentSlot = MakeCodegenScanOp(
        codegenSource,
        slotCount,
        stringLikeColumnIndices,
        rowSchemaInformationIndex,
        ExecutionBackend_);

    auto whereClause = query->WhereClause;

    auto Logger = MakeQueryLogger(query);

    auto joinGroups = GetJoinGroups(query->JoinClauses, schema);
    if (!joinGroups.empty()) {
        YT_LOG_DEBUG("Join groups: [%v]", JoinToString(joinGroups));
    }

    size_t joinIndex = 0;
    for (size_t joinGroupSize : joinGroups) {
        TConstExpressionPtr selfFilter;
        std::tie(selfFilter, whereClause) = SplitPredicateByColumnSubset(whereClause, *schema);

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

        if (auto arrayJoinClause = query->JoinClauses[joinIndex]; !arrayJoinClause->ArrayExpressions.empty()) {
            YT_ASSERT(joinGroupSize == 1);

            joinIndex++;
            Fold(static_cast<int>(EFoldingObjectType::ArrayJoinOp));

            const auto& arrayExpressions = arrayJoinClause->ArrayExpressions;
            auto renamedJoinSchema = arrayJoinClause->GetRenamedSchema();
            int arrayCount = arrayExpressions.size();

            YT_VERIFY(arrayCount == renamedJoinSchema->GetColumnCount());

            TExpressionFragments arrayAndPredicateFragments;
            auto predicateExpression = (arrayJoinClause->Predicate && !IsTrue(arrayJoinClause->Predicate))
                ? arrayJoinClause->Predicate
                : New<TLiteralExpression>(EValueType::Boolean, MakeUnversionedBooleanValue(true));
            size_t predicateId = TExpressionProfiler::Profile(
                predicateExpression,
                renamedJoinSchema,
                &arrayAndPredicateFragments);

            std::vector<size_t> arrayIds(arrayCount);
            for (int index = 0; index < arrayCount; ++index) {
                arrayIds[index] = TExpressionProfiler::Profile(
                    arrayExpressions[index],
                    schema,
                    &arrayAndPredicateFragments);
            }

            auto fragmentInfos = arrayAndPredicateFragments.ToFragmentInfos("joinedArrays");

            std::vector<int> selfJoinedColumns;
            selfJoinedColumns.reserve(arrayJoinClause->SelfJoinedColumns.size());
            for (int index = 0; index < schema->GetColumnCount(); ++index) {
                const auto& column = schema->Columns()[index];
                if (arrayJoinClause->SelfJoinedColumns.contains(column.Name())) {
                    selfJoinedColumns.push_back(index);
                }
            }

            std::vector<int> arrayJoinedColumns;
            std::vector<EValueType> rowTypes(arrayCount);
            arrayJoinedColumns.reserve(arrayJoinClause->ForeignJoinedColumns.size());
            for (int index = 0; index < renamedJoinSchema->GetColumnCount(); ++index) {
                const auto& column = renamedJoinSchema->Columns()[index];
                rowTypes[index] = column.GetWireType();
                if (arrayJoinClause->ForeignJoinedColumns.contains(column.Name())) {
                    arrayJoinedColumns.push_back(index);
                }
            }

            TArrayJoinParameters arrayJoinParameters{
                .IsLeft=arrayJoinClause->IsLeft,
                .FlattenedTypes=std::move(rowTypes),
                .SelfJoinedColumns=std::move(selfJoinedColumns),
                .ArrayJoinedColumns=std::move(arrayJoinedColumns),
            };

            int parametersIndex = Variables_->AddOpaque<TArrayJoinParameters>(std::move(arrayJoinParameters));
            Fold(parametersIndex);
            currentSlot = MakeCodegenArrayJoinOp(
                codegenSource,
                slotCount,
                currentSlot,
                fragmentInfos,
                std::move(arrayIds),
                parametersIndex,
                predicateId);
            MakeCodegenFragmentBodies(codegenSource, fragmentInfos);

            schema = arrayJoinClause->GetTableSchema(*schema);
            TSchemaProfiler::Profile(schema);
            continue;
        }

        Fold(static_cast<int>(EFoldingObjectType::JoinOp));
        TExpressionFragments equationFragments;

        size_t joinBatchSize = MaxJoinBatchSize;

        if (query->IsOrdered() && query->Offset + query->Limit < static_cast<ssize_t>(joinBatchSize)) {
            joinBatchSize = query->Offset + query->Limit;
        }

        std::vector<TSingleJoinCGParameters> parameters;
        TMultiJoinParameters joinParameters;
        parameters.reserve(joinGroupSize);
        joinParameters.Items.reserve(joinGroupSize);

        auto lastSchema = schema;
        for (; joinGroupSize > 0; ++joinIndex, --joinGroupSize) {
            const auto& joinClause = query->JoinClauses[joinIndex];

            int equationCount = joinClause->SelfEquations.size();

            std::vector<std::pair<size_t, bool>> selfKeys(equationCount);
            std::vector<EValueType> lookupKeyTypes(equationCount);
            for (int index = 0; index < equationCount; ++index) {
                const auto& [expression, evaluated] = joinClause->SelfEquations[index];
                const auto& expressionSchema = evaluated ? joinClause->Schema.Original : schema;

                selfKeys[index] = {
                    TExpressionProfiler::Profile(
                        expression,
                        expressionSchema,
                        &equationFragments,
                        evaluated),
                    evaluated,
                };
                lookupKeyTypes[index] = expression->GetWireType();
            }

            TSingleJoinCGParameters codegenParameters{
                std::move(selfKeys),
                joinClause->CommonKeyPrefix,
                joinClause->ForeignKeyPrefix,
                std::move(lookupKeyTypes)};

            Fold(joinClause->CommonKeyPrefix);
            Fold(joinClause->ForeignKeyPrefix);

            parameters.push_back(std::move(codegenParameters));

            TSingleJoinParameters singleJoinParameters;

            {
                const auto& foreignEquations = joinClause->ForeignEquations;

                // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
                auto subquery = New<TQuery>();

                subquery->Schema.Original = joinClause->Schema.Original;
                subquery->Schema.Mapping = joinClause->Schema.Mapping;

                // (join key... , other columns...)
                auto projectClause = New<TProjectClause>();

                for (const auto& column : foreignEquations) {
                    projectClause->AddProjection(column, InferName(column));
                }

                subquery->ProjectClause = projectClause;
                subquery->WhereClause = joinClause->Predicate;

                auto joinRenamedTableColumns = joinClause->GetRenamedSchema()->Columns();

                std::vector<size_t> foreignColumns;
                for (int index = 0; index < std::ssize(joinRenamedTableColumns); ++index) {
                    const auto& renamedColumn = joinRenamedTableColumns[index];
                    if (joinClause->ForeignJoinedColumns.contains(joinRenamedTableColumns[index].Name())) {
                        foreignColumns.push_back(projectClause->Projections.size());

                        projectClause->AddProjection(
                            New<TReferenceExpression>(
                                renamedColumn.LogicalType(),
                                renamedColumn.Name()),
                            renamedColumn.Name());
                    }
                };

                singleJoinParameters.KeySize = foreignEquations.size();
                singleJoinParameters.IsLeft = joinClause->IsLeft;
                singleJoinParameters.IsPartiallySorted = joinClause->ForeignKeyPrefix < foreignEquations.size();
                singleJoinParameters.ForeignColumns = std::move(foreignColumns);
                singleJoinParameters.ExecuteForeign = joinProfiler(std::move(subquery), joinClause);
            }
            joinParameters.Items.push_back(std::move(singleJoinParameters));

            lastSchema = joinClause->GetTableSchema(*lastSchema);
        }

        const auto& selfTableColumns = schema->Columns();

        std::vector<std::pair<size_t, EValueType>> primaryColumns;
        for (size_t index = 0; index < selfTableColumns.size(); ++index) {
            if (query->JoinClauses[joinIndex - 1]->SelfJoinedColumns.contains(selfTableColumns[index].Name())) {
                primaryColumns.emplace_back(index, selfTableColumns[index].GetWireType());

                Fold(index);
                Fold(static_cast<int>(selfTableColumns[index].GetWireType()));
            }
        }

        joinParameters.PrimaryRowSize = primaryColumns.size();
        joinParameters.BatchSize = joinBatchSize;

        int index = Variables_->AddOpaque<TMultiJoinParameters>(joinParameters);

        Fold(index);

        auto fragmentInfos = equationFragments.ToFragmentInfos("selfEquation");
        for (const auto& codegenParameters : parameters) {
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

    Profile(codegenSource, query, slotCount, currentSlot, schema, /*mergeMode*/ false);
}

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstFrontQueryPtr& query,
    size_t* slotCount)
{
    Fold(static_cast<int>(ExecutionBackend_));
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));

    auto schema = query->GetRenamedSchema();
    TSchemaProfiler::Profile(schema);

    auto stringLikeColumnIndices = GetStringLikeColumnIndices(query->GetReadSchema());

    int rowSchemaInformationIndex = Variables_->AddOpaque<TRowSchemaInformation>(
        TRowSchemaInformation{
            InferRowWeightWithNoStrings(query->GetReadSchema()),
            stringLikeColumnIndices,
            query->GetReadSchema()->GetColumnCount() + 1}); // NB: We use +1 for stream tag.

    size_t currentSlot = MakeCodegenScanOp(
        codegenSource,
        slotCount,
        stringLikeColumnIndices,
        rowSchemaInformationIndex,
        ExecutionBackend_);

    // Front query always perform merge.
    Profile(codegenSource, query, slotCount, currentSlot, schema, /*mergeMode*/ true);
}

size_t TQueryProfiler::Profile(
    const TNamedItem& namedExpression,
    const TTableSchemaPtr& schema,
    TExpressionFragments* fragments)
{
    Fold(static_cast<int>(ExecutionBackend_));
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));

    size_t resultId = TExpressionProfiler::Profile(namedExpression.Expression, schema, fragments);
    Fold(resultId);
    ++fragments->Items[resultId].UseCount;

    return resultId;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Profile(
    const TTableSchemaPtr& tableSchema,
    llvm::FoldingSetNodeID* id)
{
    TSchemaProfiler profiler(id);
    profiler.Profile(tableSchema);
}

TCGExpressionGenerator Profile(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    bool useCanonicalNullRelations,
    EExecutionBackend executionBackend,
    const TConstFunctionProfilerMapPtr& functionProfilers)
{
    auto profiler = TExpressionProfiler(
        id,
        variables,
        functionProfilers,
        useCanonicalNullRelations,
        executionBackend);
    auto fragments = TExpressionFragments();
    auto exprId = profiler.Profile(expr, schema, &fragments);

    return [
            =,
            fragmentInfos = fragments.ToFragmentInfos("fragment"),
            exprId = std::move(exprId)
        ] () {
            return CodegenStandaloneExpression(fragmentInfos, exprId, executionBackend);
        };
}

TCGQueryGenerator Profile(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler,
    bool useCanonicalNullRelations,
    EExecutionBackend executionBackend,
    const TConstFunctionProfilerMapPtr& functionProfilers,
    const TConstAggregateProfilerMapPtr& aggregateProfilers)
{
    auto profiler = TQueryProfiler(
        id,
        variables,
        functionProfilers,
        aggregateProfilers,
        useCanonicalNullRelations,
        executionBackend);

    size_t slotCount = 0;
    TCodegenSource codegenSource = &CodegenEmptyOp;

    if (auto derivedQuery = dynamic_cast<const TQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount, joinProfiler);
    } else if (auto derivedQuery = dynamic_cast<const TFrontQuery*>(query.Get())) {
        profiler.Profile(&codegenSource, derivedQuery, &slotCount);
    } else {
        YT_ABORT();
    }

    return [
            =,
            codegenSource = std::move(codegenSource)
        ] {
            return CodegenQuery(&codegenSource, slotCount, executionBackend);
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
