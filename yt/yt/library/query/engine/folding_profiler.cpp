#include "folding_profiler.h"
#include "cg_fragment_compiler.h"
#include "functions_cg.h"
#include "llvm_folding_set.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_visitors.h>

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
    (QueryIsOrdered)

    (CombineGroupOpWithOrderOp)
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
    void Fold(EFoldingObjectType type);
    void Fold(EValueType type);
    void Fold(EExecutionBackend backend);
    void Fold(ETotalsMode backend);
    void Fold(bool boolean);
    void Fold(int numeric);
    void Fold(size_t numeric);
    void Fold(const char* str);
    void Fold(const llvm::FoldingSetNodeID& id);

    llvm::FoldingSetNodeID* Id_;
};

void TSchemaProfiler::Fold(EFoldingObjectType type)
{
    Fold(static_cast<int>(type));
}

void TSchemaProfiler::Fold(EValueType type)
{
    Fold(static_cast<int>(type));
}

void TSchemaProfiler::Fold(EExecutionBackend backend)
{
    Fold(static_cast<int>(backend));
}

void TSchemaProfiler::Fold(ETotalsMode mode)
{
    Fold(static_cast<int>(mode));
}

void TSchemaProfiler::Fold(bool boolean)
{
    Fold(static_cast<int>(boolean));
}

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
    Fold(EFoldingObjectType::TableSchema);
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
        : TBase(builder, /*omitValues*/ false)
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
        auto conditions = TRange(DebugExpressions[id].Args.data(), whenThenExpressionsLength);
        auto results = TRange(DebugExpressions[id].Args.data() + whenThenExpressionsLength, whenThenExpressionsLength);

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
        constexpr size_t MaxColumnNameLength = 40;
        if (columnName.size() > MaxColumnNameLength) {
            columnName.resize(MaxColumnNameLength);
            for (auto& ch : columnName) {
                if (ch == '(' || ch == ')') {
                    ch = '_';
                }
            }
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
        /*forceInline*/ true);
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
        /*nullable*/ true,
        /*forceInline*/ true);
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

    int index = Variables_->AddOpaque<TSharedRange<TRange<TPIValue>>>(CopyAndConvertToPI(inExpr->Values, /*captureValues*/ false));
    int hashtableIndex = Variables_->AddOpaque<std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>>();
    fragments->DebugInfos.emplace_back(inExpr, argIds);
    fragments->Items.emplace_back(
        MakeCodegenInExpr(std::move(argIds), index, hashtableIndex, ComparerManager_),
        inExpr->GetWireType(),
        /*nullable*/ false);
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

    int index = Variables_->AddOpaque<TSharedRange<TPIRowRange>>(
        CopyAndConvertToPI(betweenExpr->Ranges, /*captureValues*/ false));
    fragments->DebugInfos.emplace_back(betweenExpr, argIds);
    fragments->Items.emplace_back(
        MakeCodegenBetweenExpr(std::move(argIds), index, ComparerManager_),
        betweenExpr->GetWireType(),
        /*nullable*/ false);
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

    int index = Variables_->AddOpaque<TSharedRange<TRange<TPIValue>>>(
        CopyAndConvertToPI(transformExpr->Values, /*captureValues*/ false));
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

            precompiledRegex = std::make_unique<re2::RE2>(re2::StringPiece(re2Pattern.data(), re2Pattern.size()), options);
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
        EExecutionBackend executionBackend,
        bool allowUnorderedGroupByWithLimit)
        : TExpressionProfiler(id, variables, functionProfilers, useCanonicalNullRelations, executionBackend)
        , AggregateProfilers_(aggregateProfilers)
        , AllowUnorderedGroupByWithLimit_(allowUnorderedGroupByWithLimit)
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
    // COMPAT(sabdenovch)
    const bool AllowUnorderedGroupByWithLimit_;

    size_t Profile(
        const TNamedItem& namedExpression,
        const TTableSchemaPtr& schema,
        TExpressionFragments* fragments);
};

////////////////////////////////////////////////////////////////////////////////

class TGroupByStreamManager
{
public:
    TGroupByStreamManager(
        bool finalMode,
        bool mergeMode,
        TCodegenSource* codegenSource,
        const TConstBaseQueryPtr& query,
        size_t* slotCount,
        size_t dummy,
        const std::vector<TCodegenAggregate>& codegenAggregates,
        const std::vector<EValueType>& keyTypes,
        const std::vector<EValueType>& stateTypes,
        const TCodegenFragmentInfosPtr& havingFragmentsInfos,
        size_t havingPredicateId,
        TCGVariables* variables,
        bool combineGroupOpWithOrderOp,
        bool allowUnorderedGroupByWithLimit)
        : FinalMode_(finalMode)
        , MergeMode_(mergeMode)
        , CodegenSource_(codegenSource)
        , Query_(query)
        , GroupClause_(query->GroupClause)
        , SlotCount_(slotCount)
        , Dummy_(dummy)
        , AddHaving_(query->HavingClause && !IsTrue(query->HavingClause))
        , CodegenAggregates_(codegenAggregates)
        , KeyTypes_(keyTypes)
        , StateTypes_(stateTypes)
        , HavingFragmentsInfos_(havingFragmentsInfos)
        , HavingPredicateId_(havingPredicateId)
        , Variables_(variables)
        , CombineGroupOpWithOrderOp_(combineGroupOpWithOrderOp)
        , AllowUnorderedGroupByWithLimit_(allowUnorderedGroupByWithLimit)
    { }

    void Process(size_t* intermediate, size_t* aggregated, size_t* delta, size_t* totals)
    {
        ConvertIntermediateToDelta(intermediate, delta);
        AccountTotals(ETotalsMode::BeforeHaving, aggregated, delta, totals);
        FilterHaving(delta);
        AccountTotals(ETotalsMode::AfterHaving, aggregated, delta, totals);
        AddDeltaToAggregated(delta, aggregated);
        FinalizeAggregatedIfNeeded(aggregated);
    }

private:
    const bool FinalMode_;
    const bool MergeMode_;

    TCodegenSource* CodegenSource_;

    const TConstBaseQueryPtr& Query_;
    const TConstGroupClausePtr& GroupClause_;

    size_t* SlotCount_;
    const size_t Dummy_;

    const bool AddHaving_;

    const std::vector<TCodegenAggregate>& CodegenAggregates_;
    const std::vector<EValueType>& KeyTypes_;
    const std::vector<EValueType>& StateTypes_;

    const TCodegenFragmentInfosPtr& HavingFragmentsInfos_;
    const size_t HavingPredicateId_;

    TCGVariables* const Variables_;

    const bool CombineGroupOpWithOrderOp_;

    // COMPAT(sabdenovch)
    const bool AllowUnorderedGroupByWithLimit_;

    // If the query uses `WITH TOTALS` together with `LIMIT`, but without `ORDER BY`,
    // we should account totals on the query coordinator side,
    // since the coordinator decides which rows will be included into the response.
    // When totals are calculated at the coordinator, the coordinator should also finalize aggregated.
    bool ShouldFinalizeAggregatesAndAccountTotalsAtCoordinator() const
    {
        return Query_->GroupClause->TotalsMode != ETotalsMode::None && Query_->IsOrdered(AllowUnorderedGroupByWithLimit_);
    }

    // We should convert intermediates to deltas at the last stage of execution (query is final), since there will be no more groupings.
    // We can also convert intermediate to deltas if the query is disjoint (when the grouping key and the primary key are identical).
    void ConvertIntermediateToDelta(size_t* intermediate, size_t* delta) const
    {
        bool boundarySegmentsAreAlsoFinal = Query_->UseDisjointGroupBy && !CombineGroupOpWithOrderOp_;

        if (boundarySegmentsAreAlsoFinal || FinalMode_) {
            *delta = MakeCodegenMergeOp(CodegenSource_, SlotCount_, *intermediate, *delta);
            *intermediate = Dummy_;
        }
    }

    void AddToTotals(size_t* stream, size_t* totals) const
    {
        size_t duplicate;
        std::tie(duplicate, *stream) = MakeCodegenDuplicateOp(
            CodegenSource_,
            SlotCount_,
            *stream);

        if (MergeMode_) {
            *totals = MakeCodegenMergeOp(CodegenSource_, SlotCount_, *totals, duplicate);
        } else {
            *totals = duplicate; // Here we have nothing to merge.
        }
    }

    void GroupTotals(size_t* totals) const
    {
        *totals = MakeCodegenGroupTotalsOp(
            CodegenSource_,
            SlotCount_,
            *totals,
            CodegenAggregates_,
            KeyTypes_,
            StateTypes_);
    }

    void FinalizeTotals(size_t* totals) const
    {
        *totals = MakeCodegenFinalizeOp(
            CodegenSource_,
            SlotCount_,
            *totals,
            GroupClause_->GroupItems.size(),
            CodegenAggregates_,
            StateTypes_);
    }

    void LimitTotalsInput(size_t* totals) const
    {
        bool considerLimit = Query_->IsOrdered(AllowUnorderedGroupByWithLimit_) && Query_->IsFinal;

        if (considerLimit) {
            int offsetId = Variables_->AddOpaque<size_t>(Query_->Offset);
            int limitId = Variables_->AddOpaque<size_t>(Query_->Limit);
            *totals = MakeCodegenOffsetLimiterOp(CodegenSource_, SlotCount_, *totals, offsetId, limitId);
        }
    }

    void AccountTotals(ETotalsMode currentTotalsMode, size_t* aggregated, size_t* delta, size_t* totals) const
    {
        if (currentTotalsMode != GroupClause_->TotalsMode) {
            return;
        }

        if (ShouldFinalizeAggregatesAndAccountTotalsAtCoordinator()) {
            if (!FinalMode_) {
                // Do nothing.
            } else {
                AddToTotals(aggregated, totals);
                AddToTotals(delta, totals);

                // This relates to the final stage of the query execution only if the totals stream was not calculated at the previous levels.
                // In this case, we should apply a limit. This will be equivalent to applying a limit to an aggregated stream.
                // That is, all rows from the aggregated result would be taken into account in totals.
                LimitTotalsInput(totals);

                GroupTotals(totals);
                FinalizeTotals(totals);
            }
        } else {
            if (!FinalMode_) {
                AddToTotals(delta, totals);
                GroupTotals(totals);
            } else {
                AddToTotals(delta, totals);
                GroupTotals(totals);
                FinalizeTotals(totals);
            }
        }
    }

    void FilterHaving(size_t* delta) const
    {
        if (!AddHaving_) {
            return;
        }

        auto types = std::vector<EValueType>();
        for (auto& it : GroupClause_->GroupItems) {
            types.push_back(it.Expression->GetWireType());
        }

        *delta = MakeCodegenFilterFinalizedOp(
            CodegenSource_,
            SlotCount_,
            *delta,
            HavingFragmentsInfos_,
            HavingPredicateId_,
            types,
            CodegenAggregates_,
            StateTypes_);
    }

    void AddDeltaToAggregated(size_t* delta, size_t* aggregated) const
    {
        if (!ShouldFinalizeAggregatesAndAccountTotalsAtCoordinator()) {
            *delta = MakeCodegenFinalizeOp(
                CodegenSource_,
                SlotCount_,
                *delta,
                GroupClause_->GroupItems.size(),
                CodegenAggregates_,
                StateTypes_);
        }

        if (MergeMode_) {
            *aggregated = MakeCodegenMergeOp(CodegenSource_, SlotCount_, *aggregated, *delta);
        } else {
            *aggregated = *delta; // Here we have nothing to merge.
        }
    }

    void FinalizeAggregatedIfNeeded(size_t* aggregated) const
    {
        if (!FinalMode_) {
            return;
        }

        if (ShouldFinalizeAggregatesAndAccountTotalsAtCoordinator()) {
            *aggregated = MakeCodegenFinalizeOp(
                CodegenSource_,
                SlotCount_,
                *aggregated,
                GroupClause_->GroupItems.size(),
                CodegenAggregates_,
                StateTypes_);
        }
    }
};

class TExpressionHasAggregatesChecker
    : public TVisitor<TExpressionHasAggregatesChecker>
{
public:
    explicit TExpressionHasAggregatesChecker(const TAggregateItemList& aggregateItems)
        : AggregateItems_(aggregateItems)
    { }

    bool Check(const TConstExpressionPtr& expression)
    {
        Found_ = false;
        Visit(expression);
        return Found_;
    }

    void OnReference(const TReferenceExpression* referenceExpr)
    {
        for (auto& item : AggregateItems_) {
            if (item.Name == referenceExpr->ColumnName) {
                Found_ = true;
            }
        }
    }

private:
    const TAggregateItemList& AggregateItems_;
    bool Found_ = false;
};

void TQueryProfiler::Profile(
    TCodegenSource* codegenSource,
    const TConstBaseQueryPtr& query,
    size_t* slotCount,
    size_t inputSlot,
    TTableSchemaPtr schema,
    bool mergeMode)
{
    Fold(ExecutionBackend_);
    size_t dummySlot = (*slotCount)++;

    size_t aggregatedSlot = dummySlot;
    size_t intermediateSlot = dummySlot;
    size_t totalsSlot = dummySlot;

    bool finalMode = query->IsFinal;

    Fold(EFoldingObjectType::FinalMode);
    Fold(finalMode);
    Fold(EFoldingObjectType::MergeMode);
    Fold(mergeMode);
    Fold(EFoldingObjectType::QueryIsOrdered);
    Fold(query->IsOrdered(AllowUnorderedGroupByWithLimit_));

    auto combineGroupOpWithOrderOp = TCodegenOrderOpInfosPtr();

    if (auto groupClause = query->GroupClause.Get()) {
        Fold(EFoldingObjectType::GroupOp);
        Fold(groupClause->CommonPrefixWithPrimaryKey);
        Fold(query->UseDisjointGroupBy);

        bool addHaving = query->HavingClause && !IsTrue(query->HavingClause);

        Fold(EFoldingObjectType::HavingOp);
        Fold(addHaving);

        Fold(EFoldingObjectType::TotalsMode);
        Fold(groupClause->TotalsMode);

        std::vector<EValueType> keyTypes;
        std::vector<EValueType> stateTypes;
        std::vector<EValueType> aggregatedTypes;

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

        for (const auto& aggregateItem : groupClause->AggregateItems) {
            Fold(EFoldingObjectType::AggregateItem);
            Fold(aggregateItem.AggregateFunction.c_str());
            Fold(aggregateItem.Name.c_str());

            const auto& aggregate = AggregateProfilers_->GetAggregate(aggregateItem.AggregateFunction);

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
            aggregatedTypes.push_back(aggregateItem.ResultType);
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
        // If the sorting key does not use any aggregate functions and the query has no `HAVING` clause,
        // sorting will be combined with grouping.
        // Thus, grouping would maintain a priority queue of grouped rows.

        if (auto orderClause = query->OrderClause.Get()) {
            auto orderOpHasAggregates = std::any_of(
                orderClause->OrderItems.begin(),
                orderClause->OrderItems.end(),
                [&] (const TOrderItem& item) {
                    return TExpressionHasAggregatesChecker(groupClause->AggregateItems).Check(item.Expression);
                });

            if (!addHaving && !orderOpHasAggregates && groupClause->TotalsMode == ETotalsMode::None) {
                Fold(EFoldingObjectType::CombineGroupOpWithOrderOp);

                auto afterGroupSchema = groupClause->GetTableSchema(query->IsFinal);

                std::vector<size_t> orderExprIds;
                std::vector<bool> isDesc;
                std::vector<EValueType> orderColumnTypes;
                TExpressionFragments orderExprFragments;
                for (const auto& item : orderClause->OrderItems) {
                    orderExprIds.push_back(
                        TExpressionProfiler::Profile(item.Expression, afterGroupSchema, &orderExprFragments));
                    Fold(item.Descending);
                    isDesc.push_back(item.Descending);
                    orderColumnTypes.push_back(item.Expression->GetWireType());
                }

                auto orderFragmentsInfos = orderExprFragments.ToFragmentInfos("orderExpression");
                orderExprFragments.DumpArgs(orderExprIds);

                auto schemaTypes = GetTypesFromSchema(*afterGroupSchema);
                for (auto type : schemaTypes) {
                    Fold(static_cast<ui8>(type));
                }

                MakeCodegenFragmentBodies(codegenSource, orderFragmentsInfos);

                combineGroupOpWithOrderOp = New<TCodegenOrderOpInfos>(
                    std::move(orderFragmentsInfos),
                    std::move(orderExprIds),
                    std::move(orderColumnTypes),
                    std::move(schemaTypes),
                    std::move(isDesc));
            }
        }

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
            aggregatedTypes,
            query->GroupClause->AllAggregatesAreFirst(),
            mergeMode,
            groupClause->TotalsMode != ETotalsMode::None,
            // Input is ordered for ordered queries and bottom fragments if CommonPrefixWithPrimaryKey > 0.
            // Prefix comparer can be used only if input is ordered.
            (!mergeMode || query->IsOrdered(AllowUnorderedGroupByWithLimit_)) && (!combineGroupOpWithOrderOp) ? groupClause->CommonPrefixWithPrimaryKey : 0,
            combineGroupOpWithOrderOp,
            ComparerManager_);

        schema = groupClause->GetTableSchema(query->IsFinal);

        size_t havingPredicateId = 0;
        TCodegenFragmentInfosPtr havingFragmentsInfos;

        if (addHaving) {
            TExpressionFragments havingExprFragments;
            havingPredicateId = TExpressionProfiler::Profile(query->HavingClause, schema, &havingExprFragments);

            havingFragmentsInfos = havingExprFragments.ToFragmentInfos("havingExpression");
            havingExprFragments.DumpArgs(std::vector<size_t>{havingPredicateId});
        }

        size_t newIntermediateSlot = fragmentSlots.Intermediate;
        size_t newAggregatedSlot = fragmentSlots.Aggregated;
        size_t newDeltaSlot = fragmentSlots.Delta;
        size_t newTotalsSlot = fragmentSlots.Totals;

        intermediateSlot = fragmentSlots.CompatIntermediate;
        aggregatedSlot = fragmentSlots.CompatAggregated;
        size_t deltaSlot = fragmentSlots.CompatDelta;
        totalsSlot = fragmentSlots.CompatTotals;

        auto manager = TGroupByStreamManager(
            finalMode,
            mergeMode,
            codegenSource,
            query,
            slotCount,
            dummySlot,
            codegenAggregates,
            keyTypes,
            stateTypes,
            havingFragmentsInfos,
            havingPredicateId,
            Variables_,
            combineGroupOpWithOrderOp != nullptr,
            AllowUnorderedGroupByWithLimit_);

        manager.Process(&newIntermediateSlot, &newAggregatedSlot, &newDeltaSlot, &newTotalsSlot);

        // COMPAT(dtorilov): Remove after 24.1 is everywhere.
        // COMPAT begin {

        // COMPAT(lukyan)
        if (finalMode || query->UseDisjointGroupBy) {
            // Boundary segments are also final
            deltaSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                deltaSlot);

            intermediateSlot = dummySlot;
        } else if (mergeMode) {
            // TODO(dtorilov): This relates to Node query level; consider removing this merge operation.
            intermediateSlot = MakeCodegenMergeOp(
                codegenSource,
                slotCount,
                intermediateSlot,
                deltaSlot);

            deltaSlot = dummySlot;
        }

        size_t keySize = groupClause->GroupItems.size();

        if (!mergeMode || finalMode) {
            if (addHaving && groupClause->TotalsMode == ETotalsMode::AfterHaving) {
                Fold(EFoldingObjectType::HavingOp);

                auto types = std::vector<EValueType>();
                for (auto& it : groupClause->GroupItems) {
                    types.push_back(it.Expression->GetWireType());
                }

                // Finalizes row to evaluate predicate and filters source values.
                deltaSlot = MakeCodegenFilterFinalizedOp(
                    codegenSource,
                    slotCount,
                    deltaSlot,
                    havingFragmentsInfos,
                    havingPredicateId,
                    types,
                    codegenAggregates,
                    stateTypes);
            }

            if (groupClause->TotalsMode != ETotalsMode::None) {
                size_t totalsSlotNew;
                std::tie(totalsSlotNew, deltaSlot) = MakeCodegenDuplicateOp(
                    codegenSource,
                    slotCount,
                    deltaSlot);

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

            deltaSlot = MakeCodegenFinalizeOp(
                codegenSource,
                slotCount,
                deltaSlot,
                keySize,
                codegenAggregates,
                stateTypes);

            if (addHaving && groupClause->TotalsMode != ETotalsMode::AfterHaving) {
                Fold(EFoldingObjectType::HavingOp);
                deltaSlot = MakeCodegenFilterOp(
                    codegenSource,
                    slotCount,
                    deltaSlot,
                    havingFragmentsInfos,
                    havingPredicateId);
            }

            if (mergeMode) {
                aggregatedSlot = MakeCodegenMergeOp(
                    codegenSource,
                    slotCount,
                    deltaSlot,
                    aggregatedSlot);
            } else {
                aggregatedSlot = deltaSlot;
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

        intermediateSlot = MakeCodegenMergeOp(codegenSource, slotCount, newIntermediateSlot, intermediateSlot);
        aggregatedSlot = MakeCodegenMergeOp(codegenSource, slotCount, newAggregatedSlot, aggregatedSlot);
        deltaSlot = MakeCodegenMergeOp(codegenSource, slotCount, newDeltaSlot, deltaSlot);
        totalsSlot = MakeCodegenMergeOp(codegenSource, slotCount, newTotalsSlot, totalsSlot);

        // COMPAT end }

        MakeCodegenFragmentBodies(codegenSource, fragmentInfos);
        if (havingFragmentsInfos) {
            MakeCodegenFragmentBodies(codegenSource, havingFragmentsInfos);
        }
    } else {
        intermediateSlot = inputSlot;
        aggregatedSlot = MakeCodegenMergeOp(
            codegenSource,
            slotCount,
            intermediateSlot,
            aggregatedSlot);
        intermediateSlot = dummySlot;
    }

    intermediateSlot = MakeCodegenOnceOp(codegenSource, slotCount, intermediateSlot);
    aggregatedSlot = MakeCodegenOnceOp(codegenSource, slotCount, aggregatedSlot);
    totalsSlot = MakeCodegenOnceOp(codegenSource, slotCount, totalsSlot);

    if (auto orderClause = query->OrderClause.Get(); orderClause && !combineGroupOpWithOrderOp) {
        Fold(EFoldingObjectType::OrderOp);

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

        aggregatedSlot = MakeCodegenOrderOp(
            codegenSource,
            slotCount,
            aggregatedSlot,
            orderFragmentsInfos,
            std::move(orderExprIds),
            std::move(orderColumnTypes),
            schemaTypes,
            std::move(isDesc),
            ComparerManager_);
        MakeCodegenFragmentBodies(codegenSource, orderFragmentsInfos);
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(EFoldingObjectType::ProjectOp);

        std::vector<size_t> projectExprIds;
        projectExprIds.reserve(projectClause->Projections.size());
        TExpressionFragments projectExprFragments;
        for (const auto& item : projectClause->Projections) {
            projectExprIds.push_back(Profile(item, schema, &projectExprFragments));
        }

        auto projectFragmentsInfos = projectExprFragments.ToFragmentInfos("projectExpression");
        projectExprFragments.DumpArgs(projectExprIds);

        // FIXME(lukyan): Do not generate ProjectOp two times.
        aggregatedSlot = MakeCodegenProjectOp(codegenSource, slotCount, aggregatedSlot, projectFragmentsInfos, projectExprIds);
        totalsSlot = MakeCodegenProjectOp(codegenSource, slotCount, totalsSlot, projectFragmentsInfos, std::move(projectExprIds));

        MakeCodegenFragmentBodies(codegenSource, projectFragmentsInfos);

        schema = projectClause->GetTableSchema();
    }

    bool considerLimit = (query->Limit < std::numeric_limits<i64>::max()) &&
        !query->OrderClause &&
        (!query->GroupClause || query->IsFinal);
    Fold(considerLimit);
    if (considerLimit) {
        // Since we have already applied filters to queries with `having` at this stage,
        // it is safe to apply `limit` to the finalSlot of grouping queries.

        int offsetId = Variables_->AddOpaque<size_t>(query->Offset);
        int limitId = Variables_->AddOpaque<size_t>(query->Limit);

        aggregatedSlot = MakeCodegenOffsetLimiterOp(codegenSource, slotCount, aggregatedSlot, offsetId, limitId);
    }

    size_t resultRowSize = schema->GetColumnCount();

    if (!finalMode) {
        if (auto groupClause = query->GroupClause.Get()) {
            {
                auto intermediateTypes = std::vector<EValueType>();

                for (const auto& item : query->GroupClause->GroupItems) {
                    intermediateTypes.emplace_back(NTableClient::GetWireType(item.Expression->LogicalType));
                }

                for (const auto& item : query->GroupClause->AggregateItems) {
                    intermediateTypes.emplace_back(item.StateType);
                }

                totalsSlot = MakeCodegenAddStreamOp(
                    codegenSource,
                    slotCount,
                    totalsSlot,
                    resultRowSize,
                    EStreamTag::Totals,
                    intermediateTypes);

                intermediateSlot = MakeCodegenAddStreamOp(
                    codegenSource,
                    slotCount,
                    intermediateSlot,
                    resultRowSize,
                    EStreamTag::Intermediate,
                    intermediateTypes);
            }

            {
                auto aggregatedTypes = std::vector<EValueType>();

                for (const auto& item : query->GroupClause->GroupItems) {
                    aggregatedTypes.emplace_back(NTableClient::GetWireType(item.Expression->LogicalType));
                }

                for (const auto& item : query->GroupClause->AggregateItems) {
                    aggregatedTypes.emplace_back(item.ResultType);
                }

                aggregatedSlot = MakeCodegenAddStreamOp(
                    codegenSource,
                    slotCount,
                    aggregatedSlot,
                    resultRowSize,
                    EStreamTag::Aggregated,
                    aggregatedTypes);
            }
        } else {
            auto schemaTypes = GetTypesFromSchema(*schema);

            aggregatedSlot = MakeCodegenAddStreamOp(
                codegenSource,
                slotCount,
                aggregatedSlot,
                resultRowSize,
                EStreamTag::Aggregated,
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
        }

        ++resultRowSize;
    }

    size_t resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, aggregatedSlot, totalsSlot);
    resultSlot = MakeCodegenMergeOp(codegenSource, slotCount, resultSlot, intermediateSlot);

    //resultSlot = MakeCodegenOnceOp(codegenSource, slotCount, resultSlot);

    Fold(EFoldingObjectType::WriteOp);
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
    Fold(ExecutionBackend_);
    Fold(EFoldingObjectType::ScanOp);

    auto schema = query->GetRenamedSchema();
    auto readSchema = query->GetReadSchema();
    TSchemaProfiler::Profile(schema);

    auto stringLikeColumnIndices = GetStringLikeColumnIndices(readSchema);

    int rowSchemaInformationIndex = Variables_->AddOpaque<TRowSchemaInformation>(
        TRowSchemaInformation{
            .RowWeightWithNoStrings = InferRowWeightWithNoStrings(query->GetReadSchema()),
            .StringLikeIndices = stringLikeColumnIndices,
            .Length = readSchema->GetColumnCount() + 0, // NB: This query has no incoming stream tag.
        });

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
            Fold(EFoldingObjectType::FilterOp);
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
            Fold(EFoldingObjectType::ArrayJoinOp);

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
                .IsLeft = arrayJoinClause->IsLeft,
                .FlattenedTypes = std::move(rowTypes),
                .SelfJoinedColumns = std::move(selfJoinedColumns),
                .ArrayJoinedColumns = std::move(arrayJoinedColumns),
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

        Fold(EFoldingObjectType::JoinOp);
        TExpressionFragments equationFragments;

        size_t joinBatchSize = MaxJoinBatchSize;

        if (query->IsOrdered(AllowUnorderedGroupByWithLimit_) && query->Offset + query->Limit < static_cast<ssize_t>(joinBatchSize)) {
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

            TSingleJoinParameters singleJoinParameters{
                .KeySize = joinClause->ForeignEquations.size(),
                .IsLeft = joinClause->IsLeft,
                .IsPartiallySorted = joinClause->ForeignKeyPrefix < singleJoinParameters.KeySize,
                .ForeignColumns = joinClause->GetForeignColumnIndices(),
                .ExecuteForeign = joinProfiler(joinIndex),
            };

            joinParameters.Items.push_back(std::move(singleJoinParameters));

            lastSchema = joinClause->GetTableSchema(*lastSchema);
        }

        const auto& selfTableColumns = schema->Columns();

        std::vector<std::pair<size_t, EValueType>> primaryColumns;
        for (size_t index = 0; index < selfTableColumns.size(); ++index) {
            if (query->JoinClauses[joinIndex - 1]->SelfJoinedColumns.contains(selfTableColumns[index].Name())) {
                primaryColumns.emplace_back(index, selfTableColumns[index].GetWireType());

                Fold(index);
                Fold(selfTableColumns[index].GetWireType());
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
        Fold(EFoldingObjectType::FilterOp);
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
    Fold(ExecutionBackend_);
    Fold(EFoldingObjectType::ScanOp);

    auto schema = query->GetRenamedSchema();
    auto readSchema = query->GetReadSchema();
    TSchemaProfiler::Profile(schema);

    auto stringLikeColumnIndices = GetStringLikeColumnIndices(readSchema);

    int rowSchemaInformationIndex = Variables_->AddOpaque<TRowSchemaInformation>(
        TRowSchemaInformation{
            .RowWeightWithNoStrings = InferRowWeightWithNoStrings(query->GetReadSchema()),
            .StringLikeIndices = stringLikeColumnIndices,
            .Length = readSchema->GetColumnCount() + 1, // NB: We use +1 for stream tag.
        });

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
    Fold(ExecutionBackend_);
    Fold(EFoldingObjectType::NamedExpression);

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
        ] {
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
    const TConstAggregateProfilerMapPtr& aggregateProfilers,
    bool allowUnorderedGroupByWithLimit)
{
    auto profiler = TQueryProfiler(
        id,
        variables,
        functionProfilers,
        aggregateProfilers,
        useCanonicalNullRelations,
        executionBackend,
        allowUnorderedGroupByWithLimit);

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
