#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "private.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

TTypeSet GetTypes(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return TTypeSet({
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any
            });
        },
        [] (i64) {
            return TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double
            });
        },
        [] (ui64) {
            return TTypeSet({
                EValueType::Uint64,
                EValueType::Double
            });
        },
        [] (double) {
            return TTypeSet({
                EValueType::Double
            });
        },
        [] (bool) {
            return TTypeSet({
                EValueType::Boolean
            });
        },
        [] (const TString&) {
            return TTypeSet({
                EValueType::String
            });
        });
}

////////////////////////////////////////////////////////////////////////////////

bool Unify(TTypeSet* genericAssignments, const TTypeSet& types)
{
    auto intersection = *genericAssignments & types;

    if (intersection.IsEmpty()) {
        return false;
    } else {
        *genericAssignments = intersection;
        return true;
    }
}

EValueType GetFrontWithCheck(const TTypeSet& typeSet, TStringBuf source)
{
    auto result = typeSet.GetFront();
    if (result == EValueType::Null) {
        THROW_ERROR_EXCEPTION("Type inference failed")
            << TErrorAttribute("actual_type", EValueType::Null)
            << TErrorAttribute("source", source);
    }
    return result;
}

TTypeSet InferFunctionTypes(
    const TFunctionTypeInferrer* inferrer,
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf functionName,
    TStringBuf source)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<int> formalArguments;
    std::optional<std::pair<int, bool>> repeatedType;
    int formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    *genericAssignments = typeConstraints;

    int argIndex = 1;
    auto arg = effectiveTypes.begin();
    auto formalArg = formalArguments.begin();
    for (;
        formalArg != formalArguments.end() && arg != effectiveTypes.end();
        arg++, formalArg++, argIndex++)
    {
        auto& constraints = (*genericAssignments)[*formalArg];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for argument %v to function %Qv: expected %Qv, got %Qv",
                argIndex,
                functionName,
                constraints,
                *arg)
                << TErrorAttribute("expression", source);
        }
    }

    bool hasNoRepeatedArgument = !repeatedType.operator bool();

    if (formalArg != formalArguments.end() ||
        (arg != effectiveTypes.end() && hasNoRepeatedArgument))
    {
        THROW_ERROR_EXCEPTION(
            "Wrong number of arguments to function %Qv: expected %v, got %v",
            functionName,
            formalArguments.size(),
            effectiveTypes.size())
            << TErrorAttribute("expression", source);
    }

    for (; arg != effectiveTypes.end(); arg++) {
        int constraintIndex = repeatedType->first;
        if (repeatedType->second) {
            constraintIndex = genericAssignments->size();
            genericAssignments->push_back((*genericAssignments)[repeatedType->first]);
        }
        auto& constraints = (*genericAssignments)[constraintIndex];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for repeated argument to function %Qv: expected %Qv, got %Qv",
                functionName,
                constraints,
                *arg)
                << TErrorAttribute("expression", source);
        }
    }

    return (*genericAssignments)[formalResultType];
}

std::vector<EValueType> RefineFunctionTypes(
    const TFunctionTypeInferrer* inferrer,
    EValueType resultType,
    int argumentCount,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf source)
{
    std::vector<TTypeSet> typeConstraints;
    std::vector<int> formalArguments;
    std::optional<std::pair<int, bool>> repeatedType;
    int formalResultType = inferrer->GetNormalizedConstraints(
        &typeConstraints,
        &formalArguments,
        &repeatedType);

    (*genericAssignments)[formalResultType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (auto& constraint : *genericAssignments) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    std::vector<EValueType> effectiveTypes;
    effectiveTypes.reserve(argumentCount);
    int argIndex = 0;
    auto formalArg = formalArguments.begin();
    for (;
        formalArg != formalArguments.end() && argIndex < argumentCount;
        ++formalArg, ++argIndex)
    {
        effectiveTypes.push_back(genericAssignmentsMin[*formalArg]);
    }

    for (; argIndex < argumentCount; ++argIndex) {
        int constraintIndex = repeatedType->first;
        if (repeatedType->second) {
            constraintIndex = genericAssignments->size() - (argumentCount - argIndex);
        }

        effectiveTypes.push_back(genericAssignmentsMin[constraintIndex]);
    }

    return effectiveTypes;
}

// 1. Init generic assignments with constraints
//    Intersect generic assignments with argument types and save them
//    Infer feasible result types
// 2. Apply result types and restrict generic assignments and argument types

void IntersectGenericsWithArgumentTypes(
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    const std::vector<int>& formalArguments,
    TStringBuf functionName,
    TStringBuf source)
{
    if (formalArguments.size() != effectiveTypes.size()) {
        THROW_ERROR_EXCEPTION("Expected %v number of arguments to function %Qv, got %v",
            formalArguments.size(),
            functionName,
            effectiveTypes.size());
    }

    for (int argIndex = 0; argIndex < std::ssize(formalArguments); ++argIndex)
    {
        auto& constraints = (*genericAssignments)[formalArguments[argIndex]];
        if (!Unify(&constraints, effectiveTypes[argIndex])) {
            THROW_ERROR_EXCEPTION("Wrong type for argument %v to function %Qv: expected %Qv, got %Qv",
                argIndex + 1,
                functionName,
                constraints,
                effectiveTypes[argIndex])
                << TErrorAttribute("expression", source);
        }
    }
}

std::vector<EValueType> RefineFunctionTypes(
    int formalResultType,
    int formalStateType,
    const std::vector<int>& formalArguments,
    EValueType resultType,
    EValueType* stateType,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf source)
{
    (*genericAssignments)[formalResultType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (const auto& constraint : *genericAssignments) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    *stateType = genericAssignmentsMin[formalStateType];

    std::vector<EValueType> effectiveTypes;
    for (int formalArgConstraint : formalArguments)
    {
        effectiveTypes.push_back(genericAssignmentsMin[formalArgConstraint]);
    }

    return effectiveTypes;
}

struct TOperatorTyper
{
    TTypeSet Constraint;
    std::optional<EValueType> ResultType;
};

TEnumIndexedArray<EBinaryOp, TOperatorTyper> BuildBinaryOperatorTypers()
{
    TEnumIndexedArray<EBinaryOp, TOperatorTyper> result;

    for (auto op : {
        EBinaryOp::Plus,
        EBinaryOp::Minus,
        EBinaryOp::Multiply,
        EBinaryOp::Divide})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
        };
    }

    for (auto op : {
        EBinaryOp::Modulo,
        EBinaryOp::LeftShift,
        EBinaryOp::RightShift,
        EBinaryOp::BitOr,
        EBinaryOp::BitAnd})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64}),
            std::nullopt
        };
    }

    for (auto op : {
        EBinaryOp::And,
        EBinaryOp::Or})
    {
        result[op] = {
            TTypeSet({EValueType::Boolean}),
            EValueType::Boolean
        };
    }

    for (auto op : {
        EBinaryOp::Equal,
        EBinaryOp::NotEqual,
        EBinaryOp::Less,
        EBinaryOp::Greater,
        EBinaryOp::LessOrEqual,
        EBinaryOp::GreaterOrEqual})
    {
        result[op] = {
            TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any}),
            EValueType::Boolean
        };
    }

    for (auto op : {EBinaryOp::Concatenate}) {
        result[op] = {
            TTypeSet({ EValueType::String, }),
            EValueType::String
        };
    }

    return result;
}

const TEnumIndexedArray<EBinaryOp, TOperatorTyper>& GetBinaryOperatorTypers()
{
    static auto result = BuildBinaryOperatorTypers();
    return result;
}

TEnumIndexedArray<EUnaryOp, TOperatorTyper> BuildUnaryOperatorTypers()
{
    TEnumIndexedArray<EUnaryOp, TOperatorTyper> result;

    for (auto op : {
        EUnaryOp::Plus,
        EUnaryOp::Minus})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
        };
    }

    result[EUnaryOp::BitNot] = {
        TTypeSet({EValueType::Int64, EValueType::Uint64}),
        std::nullopt
    };

    result[EUnaryOp::Not] = {
        TTypeSet({EValueType::Boolean}),
        std::nullopt
    };

    return result;
}

const TEnumIndexedArray<EUnaryOp, TOperatorTyper>& GetUnaryOperatorTypers()
{
    static auto result = BuildUnaryOperatorTypers();
    return result;
}

TTypeSet InferBinaryExprTypes(
    EBinaryOp opCode,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        return TTypeSet{EValueType::Boolean};
    }

    const auto& binaryOperators = GetBinaryOperatorTypers();

    *genericAssignments = binaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, lhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            lhsTypes)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    if (!Unify(genericAssignments, rhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            rhsTypes)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    TTypeSet resultTypes;
    if (binaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*binaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

std::pair<EValueType, EValueType> RefineBinaryExprTypes(
    EBinaryOp opCode,
    EValueType resultType,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource,
    TStringBuf source)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        // Empty intersection (Any, alpha) || (alpha, Any), where alpha = {bool, int, uint, double, string}
        if (lhsTypes.Get(EValueType::Any)) {
            return std::pair(EValueType::Any, GetFrontWithCheck(rhsTypes, rhsSource));
        }

        if (rhsTypes.Get(EValueType::Any)) {
            return std::pair(GetFrontWithCheck(lhsTypes, lhsSource), EValueType::Any);
        }

        THROW_ERROR_EXCEPTION("Type mismatch in expression")
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource);
    }

    const auto& binaryOperators = GetBinaryOperatorTypers();

    EValueType argType;
    if (binaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, source);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return std::pair(argType, argType);
}

TTypeSet InferUnaryExprTypes(
    EUnaryOp opCode,
    const TTypeSet& argTypes,
    TTypeSet* genericAssignments,
    TStringBuf opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    *genericAssignments = unaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, argTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            argTypes)
            << TErrorAttribute("op_source", opSource);
    }

    TTypeSet resultTypes;
    if (unaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*unaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

EValueType RefineUnaryExprTypes(
    EUnaryOp opCode,
    EValueType resultType,
    TTypeSet* genericAssignments,
    TStringBuf opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    EValueType argType;
    if (unaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, opSource);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return argType;
}


////////////////////////////////////////////////////////////////////////////////

using TExpressionGenerator = std::function<TConstExpressionPtr(EValueType)>;

struct TUntypedExpression
{
    TTypeSet FeasibleTypes;
    TExpressionGenerator Generator;
    bool IsConstant;
};

struct TExprBuilderV1
    : public TExprBuilder
{
public:
    TExprBuilderV1(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap)
        : TExprBuilder(source, functions)
        , AliasMap_(aliasMap)
    { }

    // TODO(lukyan): Move ProvideAggregateColumn and GetAggregateColumnPtr to TExprBuilderV1Base and provide callback
    // OnExpression or split into two functions (GetAggregate and SetAggregate).
    std::pair<TTypeSet, std::function<TConstExpressionPtr(EValueType)>> ProvideAggregateColumn(
        const std::string& name,
        const TAggregateTypeInferrer* aggregateItem,
        const NAst::TExpression* argument,
        const TString& subexpressionName);

    TUntypedExpression GetAggregateColumnPtr(
        const std::string& functionName,
        const TAggregateTypeInferrer* aggregateItem,
        const NAst::TExpression* argument,
        const TString& subexpressionName);

    TUntypedExpression OnExpression(
        const NAst::TExpression* expr);

    TConstExpressionPtr DoBuildTypedExpression(const NAst::TExpression* expr, TRange<EValueType> resultTypes) override;

private:
    struct ResolveNestedTypesResult
    {
        TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
        TLogicalTypePtr IntermediateType;
        TLogicalTypePtr ResultType;
    };

    const NAst::TAliasMap& AliasMap_;
    std::set<std::string> UsedAliases_;
    int Depth_ = 0;

    THashMap<std::pair<TString, EValueType>, TConstExpressionPtr> AggregateLookup_;

    ResolveNestedTypesResult ResolveNestedTypes(
        const TLogicalTypePtr& type,
        const NAst::TReference& reference);

    TConstExpressionPtr UnwrapListOrDictItemAccessor(
        const NAst::TReference& reference,
        ELogicalMetatype metaType);

    TUntypedExpression UnwrapCompositeMemberAccessor(
        const NAst::TReference& reference,
        TBaseColumn column);

    TUntypedExpression OnReference(
        const NAst::TReference& reference);

    TUntypedExpression OnFunction(
        const NAst::TFunctionExpression* functionExpr);

    TUntypedExpression OnUnaryOp(
        const NAst::TUnaryOpExpression* unaryExpr);

    TUntypedExpression MakeBinaryExpr(
        const NAst::TBinaryOpExpression* binaryExpr,
        EBinaryOp op,
        TUntypedExpression lhs,
        TUntypedExpression rhs,
        std::optional<size_t> offset);

    friend struct TBinaryOpGenerator;

    TUntypedExpression OnBinaryOp(
        const NAst::TBinaryOpExpression* binaryExpr);

    void InferArgumentTypes(
        std::vector<TConstExpressionPtr>* typedArguments,
        std::vector<EValueType>* argTypes,
        const NAst::TExpressionList& expressions,
        TStringBuf operatorName,
        TStringBuf source);

    TUntypedExpression OnInOp(
        const NAst::TInExpression* inExpr);

    TUntypedExpression OnBetweenOp(
        const NAst::TBetweenExpression* betweenExpr);

    TUntypedExpression OnTransformOp(
        const NAst::TTransformExpression* transformExpr);

    TUntypedExpression OnCaseOp(
        const NAst::TCaseExpression* caseExpr);

    TUntypedExpression OnLikeOp(
        const NAst::TLikeExpression* likeExpr);
};

std::unique_ptr<TExprBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    return std::make_unique<TExprBuilderV1>(source, functions, aliasMap);
}


// TODO(lukyan): Move ProvideAggregateColumn and GetAggregateColumnPtr to TExprBuilderV1Base and provide callback
// OnExpression or split into two functions (GetAggregate and SetAggregate).
std::pair<TTypeSet, std::function<TConstExpressionPtr(EValueType)>> TExprBuilderV1::ProvideAggregateColumn(
    const std::string& name,
    const TAggregateTypeInferrer* aggregateItem,
    const NAst::TExpression* argument,
    const TString& subexpressionName)
{
    YT_VERIFY(AfterGroupBy_);

    // TODO(lukyan): Use guard.
    AfterGroupBy_ = false;
    auto untypedOperand = OnExpression(argument);
    AfterGroupBy_ = true;

    TTypeSet constraint;
    std::optional<EValueType> stateType;
    std::optional<EValueType> resultType;

    aggregateItem->GetNormalizedConstraints(&constraint, &stateType, &resultType, name);

    TTypeSet resultTypes;
    TTypeSet genericAssignments = constraint;

    if (!Unify(&genericAssignments, untypedOperand.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in function %Qv: expected %v, actual %v",
            name,
            genericAssignments,
            untypedOperand.FeasibleTypes)
            << TErrorAttribute("source", subexpressionName);
    }

    if (resultType) {
        resultTypes = TTypeSet({*resultType});
    } else {
        resultTypes = genericAssignments;
    }

    return std::pair(resultTypes, [=, this] (EValueType type) {
        EValueType argType;
        if (resultType) {
            YT_VERIFY(!genericAssignments.IsEmpty());
            argType = GetFrontWithCheck(genericAssignments, argument->GetSource(Source_));
        } else {
            argType = type;
        }

        EValueType effectiveStateType;
        if (stateType) {
            effectiveStateType = *stateType;
        } else {
            effectiveStateType = argType;
        }

        auto typedOperand = untypedOperand.Generator(argType);

        typedOperand = ApplyRewriters(typedOperand);

        AggregateItems_->emplace_back(
            std::vector<TConstExpressionPtr>{typedOperand},
            name,
            subexpressionName,
            effectiveStateType,
            type);

        return typedOperand;
    });
}

TUntypedExpression TExprBuilderV1::GetAggregateColumnPtr(
    const std::string& functionName,
    const TAggregateTypeInferrer* aggregateItem,
    const NAst::TExpression* argument,
    const TString& subexpressionName)
{
    if (!AfterGroupBy_) {
        THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName);
    }

    auto typer = ProvideAggregateColumn(
        functionName,
        aggregateItem,
        argument,
        subexpressionName);

    TExpressionGenerator generator = [=, this] (EValueType type) -> TConstExpressionPtr {
        auto key = std::pair(subexpressionName, type);
        auto found = AggregateLookup_.find(key);
        if (found != AggregateLookup_.end()) {
            return found->second;
        } else {
            auto argExpression = typer.second(type);
            auto expr = New<TReferenceExpression>(
                MakeLogicalType(GetLogicalType(type), /*required*/ false),
                subexpressionName);
            YT_VERIFY(AggregateLookup_.emplace(key, expr).second);
            return expr;
        }
    };

    return TUntypedExpression{.FeasibleTypes=typer.first, .Generator=std::move(generator), .IsConstant=false};
}

TUntypedExpression TExprBuilderV1::OnExpression(
    const NAst::TExpression* expr)
{
    CheckStackDepth();

    ++Depth_;
    auto depthGuard = Finally([&] {
        --Depth_;
    });

    if (Depth_ > MaxExpressionDepth) {
        THROW_ERROR_EXCEPTION("Maximum expression depth exceeded")
            << TErrorAttribute("max_expression_depth", MaxExpressionDepth);
    }

    if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
        const auto& literalValue = literalExpr->Value;

        auto resultTypes = GetTypes(literalValue);
        TExpressionGenerator generator = [literalValue] (EValueType type) {
            return New<TLiteralExpression>(
                type,
                CastValueWithCheck(GetValue(literalValue), type));
        };
        return TUntypedExpression{.FeasibleTypes=resultTypes, .Generator=std::move(generator), .IsConstant=true};
    } else if (auto aliasExpr = expr->As<NAst::TAliasExpression>()) {
        return OnReference(NAst::TReference(aliasExpr->Name));
    } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
        return OnReference(referenceExpr->Reference);
    } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        return OnFunction(functionExpr);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        return OnUnaryOp(unaryExpr);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        return OnBinaryOp(binaryExpr);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        return OnInOp(inExpr);
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        return OnBetweenOp(betweenExpr);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        return OnTransformOp(transformExpr);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        return OnCaseOp(caseExpr);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        return OnLikeOp(likeExpr);
    }

    YT_ABORT();
}

TConstExpressionPtr TExprBuilderV1::DoBuildTypedExpression(
    const NAst::TExpression* expr,
    TRange<EValueType> resultTypes)
{
    TTypeSet feasibleTypes(resultTypes);

    auto expressionTyper = OnExpression(expr);
    YT_VERIFY(!expressionTyper.FeasibleTypes.IsEmpty());

    if (!Unify(&feasibleTypes, expressionTyper.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression: expected %Qv, got %Qv",
            feasibleTypes,
            expressionTyper.FeasibleTypes)
            << TErrorAttribute("source", expr->GetSource(Source_));
    }

    auto result = expressionTyper.Generator(
        GetFrontWithCheck(feasibleTypes, expr->GetSource(Source_)));

    result = ApplyRewriters(result);

    return result;
}

TExprBuilderV1::ResolveNestedTypesResult TExprBuilderV1::ResolveNestedTypes(
    const TLogicalTypePtr& type,
    const NAst::TReference& reference)
{
    TCompositeMemberAccessorPath nestedStructOrTupleItemAccessor;
    nestedStructOrTupleItemAccessor.Reserve(std::ssize(reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor));

    TLogicalTypePtr current = type;

    for (const auto& item : reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                if (current->GetMetatype() != ELogicalMetatype::Struct) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                const auto& fields = current->AsStructTypeRef().GetFields();
                for (int index = 0; index < std::ssize(fields); ++index) {
                    if (fields[index].Name == structMember) {
                        current = fields[index].Type;
                        nestedStructOrTupleItemAccessor.AppendStructMember(structMember, index);
                        return;
                    }
                }

                THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                    << TErrorAttribute("source", NAst::FormatReference(reference));
            },
            [&] (const TTupleItemIndexAccessor& itemIndex) {
                if (current->GetMetatype() != ELogicalMetatype::Tuple) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                const auto& tupleElements = current->AsTupleTypeRef().GetElements();

                if (itemIndex < 0 || itemIndex >= std::ssize(tupleElements)) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        << TErrorAttribute("source", NAst::FormatReference(reference));
                }

                current = tupleElements[itemIndex];
                nestedStructOrTupleItemAccessor.AppendTupleItem(itemIndex);
            });
    }

    auto intermediateType = current;
    auto resultType = current;

    if (reference.CompositeTypeAccessor.DictOrListItemAccessor) {
        if (current->GetMetatype() == ELogicalMetatype::List) {
            resultType = current->GetElement();
        } else if (current->GetMetatype() == ELogicalMetatype::Dict) {
            auto keyType = GetWireType(current->AsDictTypeRef().GetKey());
            if (keyType != EValueType::String) {
                THROW_ERROR_EXCEPTION("Expected string key type, but got %Qlv",
                    keyType)
                    << TErrorAttribute("source", NAst::FormatReference(reference));
            }
            resultType = current->AsDictTypeRef().GetValue();
        } else {
            THROW_ERROR_EXCEPTION("Incorrect nested item accessor")
                << TErrorAttribute("source", NAst::FormatReference(reference));
        }
    }

    return {std::move(nestedStructOrTupleItemAccessor), std::move(intermediateType), std::move(resultType)};
}

TConstExpressionPtr TExprBuilderV1::UnwrapListOrDictItemAccessor(
    const NAst::TReference& reference,
    ELogicalMetatype metaType)
{
    if (!reference.CompositeTypeAccessor.DictOrListItemAccessor.has_value()) {
        return {};
    }

    auto itemIndex = *reference.CompositeTypeAccessor.DictOrListItemAccessor;

    if (std::ssize(itemIndex) != 1) {
        THROW_ERROR_EXCEPTION("Expression inside of the list or dict item accessor should be scalar")
            << TErrorAttribute("source", NAst::FormatReference(reference));
    }

    auto resultTypes = TTypeSet{};
    if (metaType == ELogicalMetatype::List) {
        resultTypes = TTypeSet{EValueType::Int64};
    } else if (metaType == ELogicalMetatype::Dict) {
        resultTypes = TTypeSet{EValueType::String};
    } else {
        YT_ABORT();
    }

    auto untypedExpression = OnExpression(itemIndex.front());
    if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Incorrect type inside of the list or dict item accessor")
            << TErrorAttribute("source", NAst::FormatReference(reference))
            << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
            << TErrorAttribute("expected_type", ToString(resultTypes));
    }

    if (metaType == ELogicalMetatype::List) {
        return untypedExpression.Generator(EValueType::Int64);
    } else if (metaType == ELogicalMetatype::Dict) {
        return untypedExpression.Generator(EValueType::String);
    } else {
        YT_ABORT();
    }
}

TUntypedExpression TExprBuilderV1::UnwrapCompositeMemberAccessor(
    const NAst::TReference& reference,
    TBaseColumn column)
{
    auto columnType = column.LogicalType;
    auto columnReference = New<TReferenceExpression>(columnType, column.Name);

    if (reference.CompositeTypeAccessor.IsEmpty()) {
        auto generator = [columnReference] (EValueType /*type*/) {
            return columnReference;
        };

        return {TTypeSet({GetWireType(columnType)}), std::move(generator), /*IsConstant*/ false};
    }

    auto resolved = ResolveNestedTypes(columnType, reference);
    auto listOrDictItemAccessor = UnwrapListOrDictItemAccessor(reference, resolved.IntermediateType->GetMetatype());

    auto memberAccessor = New<TCompositeMemberAccessorExpression>(
        resolved.ResultType,
        columnReference,
        std::move(resolved.NestedStructOrTupleItemAccessor),
        listOrDictItemAccessor);

    auto generator = [memberAccessor] (EValueType /*type*/) {
        return memberAccessor;
    };

    return {TTypeSet({GetWireType(resolved.ResultType)}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExprBuilderV1::OnReference(const NAst::TReference& reference)
{
    if (AfterGroupBy_) {
        if (auto column = GetColumnPtr(reference)) {
            return UnwrapCompositeMemberAccessor(reference, *column);
        }
    }

    if (!reference.TableName) {
        const auto& columnName = reference.ColumnName;
        auto found = AliasMap_.find(columnName);

        if (found != AliasMap_.end()) {
            // try InferName(found, expand aliases = true)

            if (UsedAliases_.insert(columnName).second) {
                auto aliasExpr = OnExpression(found->second);
                UsedAliases_.erase(columnName);
                return aliasExpr;
            }
        }
    }

    if (!AfterGroupBy_) {
        if (auto column = GetColumnPtr(reference)) {
            return UnwrapCompositeMemberAccessor(reference, *column);
        }
    }

    THROW_ERROR_EXCEPTION("Undefined reference %Qv",
        NAst::InferColumnName(reference));
}

TUntypedExpression TExprBuilderV1::OnFunction(const NAst::TFunctionExpression* functionExpr)
{
    auto functionName = functionExpr->FunctionName;
    functionName.to_lower();

    const auto& descriptor = Functions_->GetFunction(functionName);

    // TODO(lukyan): Merge TAggregateFunctionTypeInferrer and TAggregateTypeInferrer.

    if (const auto* aggregateFunction = descriptor->As<TAggregateFunctionTypeInferrer>()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        std::vector<TTypeSet> argTypes;
        argTypes.reserve(functionExpr->Arguments.size());
        std::vector<TTypeSet> genericAssignments;
        std::vector<TExpressionGenerator> operandTypers;
        operandTypers.reserve(functionExpr->Arguments.size());
        std::vector<int> formalArguments;

        if (!AfterGroupBy_) {
            THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName);
        }

        AfterGroupBy_ = false;
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
            argTypes.push_back(untypedArgument.FeasibleTypes);
            operandTypers.push_back(untypedArgument.Generator);
        }
        AfterGroupBy_ = true;

        // TODO(lukyan): Move following code into GetAggregateColumnPtr or remove GetAggregateColumnPtr function.

        int stateConstraintIndex;
        int resultConstraintIndex;

        std::tie(stateConstraintIndex, resultConstraintIndex) = aggregateFunction->GetNormalizedConstraints(
            &genericAssignments,
            &formalArguments);
        IntersectGenericsWithArgumentTypes(
            argTypes,
            &genericAssignments,
            formalArguments,
            functionName,
            functionExpr->GetSource(Source_));

        auto resultTypes = genericAssignments[resultConstraintIndex];

        TExpressionGenerator generator = [
            this,
            stateConstraintIndex,
            resultConstraintIndex,
            functionName = std::move(functionName),
            subexpressionName = std::move(subexpressionName),
            operandTypers = std::move(operandTypers),
            genericAssignments = std::move(genericAssignments),
            formalArguments = std::move(formalArguments),
            source = functionExpr->GetSource(Source_)
        ] (EValueType type) mutable -> TConstExpressionPtr {
            auto key = std::pair(subexpressionName, type);
            auto foundCached = AggregateLookup_.find(key);
            if (foundCached != AggregateLookup_.end()) {
                return foundCached->second;
            }

            EValueType stateType;
            auto effectiveTypes = RefineFunctionTypes(
                resultConstraintIndex,
                stateConstraintIndex,
                formalArguments,
                type,
                &stateType,
                &genericAssignments,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));

                typedOperands.back() = ApplyRewriters(typedOperands.back());
            }

            AggregateItems_->emplace_back(
                typedOperands,
                functionName,
                subexpressionName,
                stateType,
                type);

            auto expr = New<TReferenceExpression>(
                MakeLogicalType(GetLogicalType(type), /*required*/ false),
                subexpressionName);
            AggregateLookup_.emplace(key, expr);

            return expr;
        };

        return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
    } else if (const auto* aggregateItem = descriptor->As<TAggregateTypeInferrer>()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        try {
            if (functionExpr->Arguments.size() != 1) {
                THROW_ERROR_EXCEPTION("Aggregate function %Qv must have exactly one argument", functionName);
            }

            auto aggregateColumn = GetAggregateColumnPtr(
                functionName,
                aggregateItem,
                functionExpr->Arguments.front(),
                subexpressionName);

            return aggregateColumn;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating aggregate")
                << TErrorAttribute("source", functionExpr->GetSource(Source_))
                << ex;
        }
    } else if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
        std::vector<TTypeSet> argTypes;
        std::vector<TExpressionGenerator> operandTypers;
        argTypes.reserve(functionExpr->Arguments.size());
        operandTypers.reserve(functionExpr->Arguments.size());
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
            argTypes.push_back(untypedArgument.FeasibleTypes);
            operandTypers.push_back(untypedArgument.Generator);
        }

        std::vector<TTypeSet> genericAssignments;
        auto resultTypes = InferFunctionTypes(
            regularFunction,
            argTypes,
            &genericAssignments,
            functionName,
            functionExpr->GetSource(Source_));

        TExpressionGenerator generator = [
            functionName,
            regularFunction,
            operandTypers,
            genericAssignments,
            source = functionExpr->GetSource(Source_)
        ] (EValueType type) mutable {
            auto effectiveTypes = RefineFunctionTypes(
                regularFunction,
                type,
                operandTypers.size(),
                &genericAssignments,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            typedOperands.reserve(std::ssize(effectiveTypes));
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));
            }

            return New<TFunctionExpression>(type, functionName, std::move(typedOperands));
        };

        return TUntypedExpression{.FeasibleTypes=resultTypes, .Generator=std::move(generator), .IsConstant=false};
    } else {
        YT_ABORT();
    }
}

TUntypedExpression TExprBuilderV1::OnUnaryOp(const NAst::TUnaryOpExpression* unaryExpr)
{
    if (unaryExpr->Operand.size() != 1) {
        THROW_ERROR_EXCEPTION(
            "Unary operator %Qv must have exactly one argument",
            unaryExpr->Opcode);
    }

    auto untypedOperand = OnExpression(unaryExpr->Operand.front());

    TTypeSet genericAssignments;
    auto resultTypes = InferUnaryExprTypes(
        unaryExpr->Opcode,
        untypedOperand.FeasibleTypes,
        &genericAssignments,
        unaryExpr->Operand.front()->GetSource(Source_));

    if (untypedOperand.IsConstant) {
        auto value = untypedOperand.Generator(untypedOperand.FeasibleTypes.GetFront());
        if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, value)) {
            TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                return New<TLiteralExpression>(
                    type,
                    CastValueWithCheck(*foldedExpr, type));
            };
            return TUntypedExpression{.FeasibleTypes=resultTypes, .Generator=std::move(generator), .IsConstant=true};
        }
    }

    TExpressionGenerator generator = [
        op = unaryExpr->Opcode,
        untypedOperand,
        genericAssignments,
        opSource = unaryExpr->Operand.front()->GetSource(Source_)
    ] (EValueType type) mutable {
        auto argType = RefineUnaryExprTypes(
            op,
            type,
            &genericAssignments,
            opSource);
        return New<TUnaryOpExpression>(type, op, untypedOperand.Generator(argType));
    };
    return TUntypedExpression{.FeasibleTypes=resultTypes, .Generator=std::move(generator), .IsConstant=false};
}

TUntypedExpression TExprBuilderV1::MakeBinaryExpr(
    const NAst::TBinaryOpExpression* binaryExpr,
    EBinaryOp op,
    TUntypedExpression lhs,
    TUntypedExpression rhs,
    std::optional<size_t> offset)
{
    TTypeSet genericAssignments;

    auto lhsSource = offset ? binaryExpr->Lhs[*offset]->GetSource(Source_) : "";
    auto rhsSource = offset ? binaryExpr->Rhs[*offset]->GetSource(Source_) : "";

    auto resultTypes = InferBinaryExprTypes(
        op,
        lhs.FeasibleTypes,
        rhs.FeasibleTypes,
        &genericAssignments,
        lhsSource,
        rhsSource);

    if (lhs.IsConstant && rhs.IsConstant) {
        auto lhsValue = lhs.Generator(lhs.FeasibleTypes.GetFront());
        auto rhsValue = rhs.Generator(rhs.FeasibleTypes.GetFront());
        if (auto foldedExpr = FoldConstants(op, lhsValue, rhsValue)) {
            TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                return New<TLiteralExpression>(
                    type,
                    CastValueWithCheck(*foldedExpr, type));
            };
            return TUntypedExpression{.FeasibleTypes=resultTypes, .Generator=std::move(generator), .IsConstant=true};
        }
    }

    TExpressionGenerator generator = [
        op,
        lhs,
        rhs,
        genericAssignments,
        lhsSource,
        rhsSource,
        source = binaryExpr->GetSource(Source_)
    ] (EValueType type) mutable {
        auto argTypes = RefineBinaryExprTypes(
            op,
            type,
            lhs.FeasibleTypes,
            rhs.FeasibleTypes,
            &genericAssignments,
            lhsSource,
            rhsSource,
            source);

        return New<TBinaryOpExpression>(
            type,
            op,
            lhs.Generator(argTypes.first),
            rhs.Generator(argTypes.second));
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

struct TBinaryOpGenerator
{
    TExprBuilderV1& Builder;
    const NAst::TBinaryOpExpression* BinaryExpr;

    TUntypedExpression Do(size_t keySize, EBinaryOp op)
    {
        YT_VERIFY(keySize > 0);
        size_t offset = keySize - 1;

        auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
        auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

        auto result = Builder.MakeBinaryExpr(BinaryExpr, op, std::move(untypedLhs), std::move(untypedRhs), offset);

        while (offset > 0) {
            --offset;
            auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
            auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

            auto eq = Builder.MakeBinaryExpr(
                BinaryExpr,
                op == EBinaryOp::NotEqual ? EBinaryOp::Or : EBinaryOp::And,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    op == EBinaryOp::NotEqual ? EBinaryOp::NotEqual : EBinaryOp::Equal,
                    untypedLhs,
                    untypedRhs,
                    offset),
                std::move(result),
                std::nullopt);

            if (op == EBinaryOp::Equal || op == EBinaryOp::NotEqual) {
                result = eq;
                continue;
            }

            EBinaryOp strongOp = op;
            if (op == EBinaryOp::LessOrEqual) {
                strongOp = EBinaryOp::Less;
            } else if (op == EBinaryOp::GreaterOrEqual)  {
                strongOp = EBinaryOp::Greater;
            }

            result = Builder.MakeBinaryExpr(
                BinaryExpr,
                EBinaryOp::Or,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    strongOp,
                    std::move(untypedLhs),
                    std::move(untypedRhs),
                    offset),
                std::move(eq),
                std::nullopt);
        }

        return result;
    }
};

TUntypedExpression TExprBuilderV1::OnBinaryOp(
    const NAst::TBinaryOpExpression* binaryExpr)
{
    if (IsRelationalBinaryOp(binaryExpr->Opcode)) {
        if (binaryExpr->Lhs.size() != binaryExpr->Rhs.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                binaryExpr->Lhs.size(),
                binaryExpr->Rhs.size())
                << TErrorAttribute("source", binaryExpr->GetSource(Source_));
        }

        int keySize = binaryExpr->Lhs.size();
        return TBinaryOpGenerator{*this, binaryExpr}.Do(keySize, binaryExpr->Opcode);
    } else {
        if (binaryExpr->Lhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Lhs));
        }

        if (binaryExpr->Rhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Rhs));
        }

        auto untypedLhs = OnExpression(binaryExpr->Lhs.front());
        auto untypedRhs = OnExpression(binaryExpr->Rhs.front());

        return MakeBinaryExpr(binaryExpr, binaryExpr->Opcode, std::move(untypedLhs), std::move(untypedRhs), 0);
    }
}

void TExprBuilderV1::InferArgumentTypes(
    std::vector<TConstExpressionPtr>* typedArguments,
    std::vector<EValueType>* argTypes,
    const NAst::TExpressionList& expressions,
    TStringBuf operatorName,
    TStringBuf source)
{
    std::unordered_set<std::string> columnNames;

    for (const auto& argument : expressions) {
        auto untypedArgument = OnExpression(argument);

        auto argType = GetFrontWithCheck(untypedArgument.FeasibleTypes, argument->GetSource(Source_));
        auto typedArgument = untypedArgument.Generator(argType);

        typedArguments->push_back(typedArgument);
        argTypes->push_back(argType);
        if (auto reference = typedArgument->As<TReferenceExpression>()) {
            if (!columnNames.insert(reference->ColumnName).second) {
                THROW_ERROR_EXCEPTION("%v operator has multiple references to column %Qv",
                    operatorName,
                    reference->ColumnName)
                    << TErrorAttribute("source", source);
            }
        }
    }
}

TUntypedExpression TExprBuilderV1::OnInOp(
    const NAst::TInExpression* inExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = inExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        inExpr->Expr,
        "IN",
        inExpr->GetSource(Source_));

    for (auto type : argTypes) {
        if (IsAnyOrComposite(type)) {
            THROW_ERROR_EXCEPTION("Cannot use expression of type %Qlv with IN operator", type)
                << TErrorAttribute("source", source);
        }
    }

    auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, source);
    auto result = New<TInExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExprBuilderV1::OnBetweenOp(
    const NAst::TBetweenExpression* betweenExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = betweenExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        betweenExpr->Expr,
        "BETWEEN",
        source);

    auto capturedRows = LiteralRangesListToRows(betweenExpr->Values, argTypes, source);
    auto result = New<TBetweenExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExprBuilderV1::OnTransformOp(
    const NAst::TTransformExpression* transformExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = transformExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        transformExpr->Expr,
        "TRANSFORM",
        source);

    if (transformExpr->From.size() != transformExpr->To.size()) {
        THROW_ERROR_EXCEPTION("Size mismatch for source and result arrays in TRANSFORM operator")
            << TErrorAttribute("source", source);
    }

    TTypeSet resultTypes({
        EValueType::Null,
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Any});

    for (const auto& tuple : transformExpr->To) {
        if (tuple.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", source);
        }

        auto valueTypes = GetTypes(tuple.front());

        if (!Unify(&resultTypes, valueTypes)) {
            THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                << TErrorAttribute("source", source)
                << TErrorAttribute("actual_type", ToString(valueTypes))
                << TErrorAttribute("expected_type", ToString(resultTypes));
        }
    }

    const auto& defaultExpr = transformExpr->DefaultExpr;

    TConstExpressionPtr defaultTypedExpr;

    EValueType resultType;
    if (defaultExpr) {
        if (defaultExpr->size() != 1) {
            THROW_ERROR_EXCEPTION("Default expression must scalar")
                << TErrorAttribute("source", source);
        }

        auto untypedArgument = OnExpression(defaultExpr->front());

        if (!Unify(&resultTypes, untypedArgument.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in default expression: expected %Qlv, got %Qlv",
                resultTypes,
                untypedArgument.FeasibleTypes)
                << TErrorAttribute("source", source);
        }

        resultType = GetFrontWithCheck(resultTypes, source);

        defaultTypedExpr = untypedArgument.Generator(resultType);
    } else {
        resultType = GetFrontWithCheck(resultTypes, source);
    }

    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    rows.reserve(std::ssize(transformExpr->From));

    for (int index = 0; index < std::ssize(transformExpr->From); ++index) {
        const auto& sourceTuple = transformExpr->From[index];
        if (sourceTuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                << TErrorAttribute("source", source);
        }
        for (int i = 0; i < std::ssize(sourceTuple); ++i) {
            auto valueType = GetType(sourceTuple[i]);
            auto value = GetValue(sourceTuple[i]);

            if (valueType == EValueType::Null) {
                value = MakeUnversionedSentinelValue(EValueType::Null);
            } else if (valueType != argTypes[i]) {
                if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                    value = CastValueWithCheck(value, argTypes[i]);
                } else {
                    THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("actual_type", valueType)
                        << TErrorAttribute("expected_type", argTypes[i]);
                }
            }
            rowBuilder.AddValue(value);
        }

        const auto& resultTuple = transformExpr->To[index];

        YT_VERIFY(resultTuple.size() == 1);
        auto value = CastValueWithCheck(GetValue(resultTuple.front()), resultType);
        rowBuilder.AddValue(value);

        rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end(), [argCount = argTypes.size()] (TRow lhs, TRow rhs) {
        return CompareRows(lhs, rhs, argCount) < 0;
    });

    auto capturedRows = MakeSharedRange(std::move(rows), std::move(rowBuffer));
    auto result = New<TTransformExpression>(
        resultType,
        std::move(typedArguments),
        std::move(capturedRows),
        std::move(defaultTypedExpr));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExprBuilderV1::OnCaseOp(const NAst::TCaseExpression* caseExpr)
{
    auto source = caseExpr->GetSource(Source_);

    TUntypedExpression untypedOperand;
    TTypeSet operandTypes;
    bool hasOptionalOperand = false;
    {
        if (caseExpr->OptionalOperand) {
            if (caseExpr->OptionalOperand->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE should be scalar")
                    << TErrorAttribute("source", source);
            }

            untypedOperand = OnExpression(caseExpr->OptionalOperand->front());
            operandTypes = untypedOperand.FeasibleTypes;
            hasOptionalOperand = true;
        }
    }

    std::vector<TUntypedExpression> untypedConditions;
    untypedConditions.reserve(caseExpr->WhenThenExpressions.size());
    EValueType conditionType{};
    std::optional<EValueType> operandType;
    {
        TTypeSet conditionTypes;
        if (hasOptionalOperand) {
            conditionTypes = operandTypes;
        } else {
            conditionTypes = TTypeSet({EValueType::Boolean});
        }

        for (auto& [condition, _] : caseExpr->WhenThenExpressions) {
            if (condition.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be scalar")
                    << TErrorAttribute("source", source);
            }

            auto untypedExpression = OnExpression(condition.front());
            if (!Unify(&conditionTypes, untypedExpression.FeasibleTypes)) {
                if (hasOptionalOperand) {
                    THROW_ERROR_EXCEPTION("Types mismatch in CASE WHEN expression")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                        << TErrorAttribute("expected_type", ToString(conditionTypes));
                } else {
                    THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be boolean")
                        << TErrorAttribute("source", source);
                }
            }

            untypedConditions.push_back(std::move(untypedExpression));
        }

        conditionType = GetFrontWithCheck(conditionTypes, source);

        if (hasOptionalOperand) {
            operandType = conditionType;
        }
    }

    std::vector<TUntypedExpression> untypedResults;
    untypedResults.reserve(caseExpr->WhenThenExpressions.size());
    TUntypedExpression untypedDefault{};
    EValueType resultType{};
    {
        TTypeSet resultTypes({
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any});

        for (auto& [_, result] : caseExpr->WhenThenExpressions) {
            if (result.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE THEN should be scalar")
                    << TErrorAttribute("source", source);
            }

            auto untypedExpression = OnExpression(result.front());
            if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN expression")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                    << TErrorAttribute("expected_type", ToString(resultTypes));
            }

            untypedResults.push_back(std::move(untypedExpression));
        }

        if (caseExpr->DefaultExpression) {
            if (caseExpr->DefaultExpression->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE ELSE should be scalar")
                    << TErrorAttribute("source", source);
            }

            untypedDefault = OnExpression(caseExpr->DefaultExpression->front());
            if (!Unify(&resultTypes, untypedDefault.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE ELSE expression")
                    << TErrorAttribute("source", source)
                    << TErrorAttribute("actual_type", ToString(untypedDefault.FeasibleTypes))
                    << TErrorAttribute("expected_type", ToString(resultTypes));
            }
        }

        resultType = GetFrontWithCheck(resultTypes, source);
    }

    TConstExpressionPtr typedOptionalOperand;
    std::vector<TWhenThenExpressionPtr> typedWhenThenExpressions;
    typedWhenThenExpressions.reserve(caseExpr->WhenThenExpressions.size());
    TConstExpressionPtr typedDefaultExpression;
    {
        if (hasOptionalOperand) {
            typedOptionalOperand = untypedOperand.Generator(*operandType);
        }

        for (size_t index = 0; index < untypedConditions.size(); ++index) {
            typedWhenThenExpressions.push_back(New<TWhenThenExpression>(
                untypedConditions[index].Generator(conditionType),
                untypedResults[index].Generator(resultType)));
        }

        if (caseExpr->DefaultExpression) {
            typedDefaultExpression = untypedDefault.Generator(resultType);
        }
    }

    auto result = New<TCaseExpression>(
        resultType,
        std::move(typedOptionalOperand),
        std::move(typedWhenThenExpressions),
        std::move(typedDefaultExpression));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };

    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExprBuilderV1::OnLikeOp(const NAst::TLikeExpression* likeExpr)
{
    auto source = likeExpr->GetSource(Source_);

    if (likeExpr->Opcode == EStringMatchOp::Regex && likeExpr->EscapeCharacter) {
        THROW_ERROR_EXCEPTION("ESCAPE should not be used together with REGEXP (RLIKE)")
            << TErrorAttribute("source", source);
    }

    auto makeTypedStringExpression = [this] (
        const NAst::TExpressionList& expression,
        TStringBuf name,
        TStringBuf source)
    {
        auto stringTypes = TTypeSet({EValueType::String,});

        if (expression.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar %Qv expression",
                name)
                << TErrorAttribute("source", source);
        }

        auto untypedExpression = OnExpression(expression.front());
        if (!Unify(&stringTypes, untypedExpression.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Types mismatch in %v", name)
                << TErrorAttribute("source", source)
                << TErrorAttribute("actual_type", ToString(untypedExpression.FeasibleTypes))
                << TErrorAttribute("expected_type", ToString(stringTypes));
        }

        return untypedExpression.Generator(EValueType::String);
    };

    auto typedText = makeTypedStringExpression(likeExpr->Text, "LIKE matched value", source);
    auto typedPattern = makeTypedStringExpression(likeExpr->Pattern, "LIKE pattern", source);

    TConstExpressionPtr typedEscapeCharacter;
    if (likeExpr->EscapeCharacter) {
        typedEscapeCharacter = makeTypedStringExpression(likeExpr->EscapeCharacter.value(), "escape character", source);
    }

    auto result = New<TLikeExpression>(
        std::move(typedText),
        likeExpr->Opcode,
        std::move(typedPattern),
        std::move(typedEscapeCharacter));

    TExpressionGenerator generator = [result] (EValueType /*type*/) {
        return result;
    };

    return TUntypedExpression{TTypeSet({EValueType::Boolean}), std::move(generator), /*IsConstant*/ false};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
