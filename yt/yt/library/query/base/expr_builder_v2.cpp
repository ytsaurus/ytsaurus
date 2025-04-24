#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "private.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// Types (t1, t2) unified to t2 if there is cast t1 -> t2.
// Types (t1, t2, t3) are not unitified to t3 with casts t1 -> t2 and t2 -> t3 because no direct cast t1 -> t3.
// Types (t1, t2, t3) unified to t2 with t2 -> t3 and t1 -> t3.

TTypeId UnifyTypes(TStringBuf name, TTypingCtx* typingCtx, TRange<TTypeId> types)
{
    // One generic parameter.
    std::vector<TTypeId> signature(std::ssize(types), -1);

    return typingCtx->InferFunctionType(name, {TTypingCtx::TFunctionSignature(signature)}, types, 0)[0];
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr CreateCoercion(EValueType type, TConstExpressionPtr operand)
{
    if (const auto* literalExpr = operand->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;

        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(value, type));
    }

    const char* castName;
    if (type == EValueType::Int64) {
        castName = "int64";
    } else if (type == EValueType::Uint64) {
        castName = "uint64";
    } else if (type == EValueType::Double) {
        castName = "double";
    } else if (type == EValueType::Any) {
        castName = "to_any";
    } else {
        THROW_ERROR_EXCEPTION("No cast from %v to %v", *operand->LogicalType, type);
    }

    return New<TFunctionExpression>(
        type,
        castName,
        std::vector<TConstExpressionPtr>{operand});
}

////////////////////////////////////////////////////////////////////////////////

struct TExprBuilderV2
    : public TExprBuilder
{
public:
    TExprBuilderV2(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap)
        : TExprBuilder(source, functions)
        , AliasMap_(aliasMap)
    { }

    TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr, TRange<EValueType> /*resultTypes*/) override;

    void AddTable(TNameSource nameSource) override
    {
        ColumnResolver_.AddTable(std::move(nameSource));
    }

    TLogicalTypePtr ResolveColumn(const NAst::TColumnReference& reference) override
    {
        return ColumnResolver_.Resolve(reference);
    }

    void PopulateAllColumns() override
    {
        ColumnResolver_.PopulateAllColumns();
    }

    void Finish() override
    {
        ColumnResolver_.Finish();
    }

    TLogicalTypePtr GetColumnType(const NAst::TColumnReference& reference)
    {
        if (AfterGroupBy_) {
            // Search other way after group by.
            if (reference.TableName) {
                return nullptr;
            }

            for (int index = 0; index < std::ssize(*GroupItems_); ++index) {
                const auto& item = (*GroupItems_)[index];
                if (item.Name == reference.ColumnName) {
                    return item.Expression->LogicalType;
                }
            }
            return nullptr;
        }

        return ColumnResolver_.Resolve(reference);
    }

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems) override
    {
        YT_VERIFY(!GroupItems_ && !AggregateItems_);

        GroupItems_ = groupItems;
        AggregateItems_ = aggregateItems;
        AfterGroupBy_ = true;
    }

    TString InferGroupItemName(
        const TConstExpressionPtr& /*typedExpression*/,
        const NAst::TExpression& expressionsAst) override
    {
        return InferColumnName(expressionsAst);
    }

private:
    struct ResolveNestedTypesResult
    {
        TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
        TLogicalTypePtr IntermediateType;
        TLogicalTypePtr ResultType;
    };

    TTypingCtx TypingCtx_;
    const NAst::TAliasMap& AliasMap_;
    std::set<std::string> UsedAliases_;
    int Depth_ = 0;

    THashMap<TString, TConstExpressionPtr> AggregateLookup_;

    TReferenceResolver ColumnResolver_;

    const TNamedItemList* GroupItems_ = nullptr;
    // TODO: Enrich TMappedSchema with alias and keep here pointers to TMappedSchema.

    TAggregateItemList* AggregateItems_ = nullptr;

    bool AfterGroupBy_ = false;

    TConstExpressionPtr OnExpression(
        const NAst::TExpression* expr);

    ResolveNestedTypesResult ResolveNestedTypes(
        const TLogicalTypePtr& type,
        const NAst::TReference& reference);

    TConstExpressionPtr UnwrapListOrDictItemAccessor(
        const NAst::TReference& reference,
        ELogicalMetatype metaType);

    TConstExpressionPtr UnwrapCompositeMemberAccessor(
        const NAst::TReference& reference,
        const TLogicalTypePtr& type);

    TConstExpressionPtr OnReference(
        const NAst::TReference& reference);

    TConstExpressionPtr OnFunction(
        const NAst::TFunctionExpression* functionExpr);

    TConstExpressionPtr OnUnaryOp(
        const NAst::TUnaryOpExpression* unaryExpr);

    TConstExpressionPtr MakeBinaryExpr(
        const NAst::TBinaryOpExpression* binaryExpr,
        EBinaryOp op,
        TConstExpressionPtr lhs,
        TConstExpressionPtr rhs,
        std::optional<size_t> offset);

    friend struct TBinaryOpGeneratorV2;

    TConstExpressionPtr OnBinaryOp(
        const NAst::TBinaryOpExpression* binaryExpr);

    void InferArgumentTypes(
        std::vector<TConstExpressionPtr>* typedArguments,
        std::vector<EValueType>* argTypes,
        const NAst::TExpressionList& expressions,
        TStringBuf operatorName,
        TStringBuf source);

    TConstExpressionPtr OnInOp(
        const NAst::TInExpression* inExpr);

    TConstExpressionPtr OnBetweenOp(
        const NAst::TBetweenExpression* betweenExpr);

    TConstExpressionPtr OnTransformOp(
        const NAst::TTransformExpression* transformExpr);

    TConstExpressionPtr OnCaseOp(
        const NAst::TCaseExpression* caseExpr);

    TConstExpressionPtr OnLikeOp(
        const NAst::TLikeExpression* likeExpr);
};

std::unique_ptr<TExprBuilder> CreateExpressionBuilderV2(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    return std::make_unique<TExprBuilderV2>(source, functions, aliasMap);
}

TConstExpressionPtr TExprBuilderV2::OnExpression(
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

        auto type = GetType(literalValue);

        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(GetValue(literalValue), type));
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

TConstExpressionPtr TExprBuilderV2::DoBuildTypedExpression(
    const NAst::TExpression* expr, TRange<EValueType> /*resultTypes*/)
{
    auto result = OnExpression(expr);

    // TODO(lukyan): Rewrite inplace.
    return ApplyRewriters(result);
}

TExprBuilderV2::ResolveNestedTypesResult TExprBuilderV2::ResolveNestedTypes(
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

TConstExpressionPtr TExprBuilderV2::UnwrapListOrDictItemAccessor(
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

    EValueType expectedType;
    if (metaType == ELogicalMetatype::List) {
        expectedType = EValueType::Int64;
    } else if (metaType == ELogicalMetatype::Dict) {
        expectedType = EValueType::String;
    } else {
        YT_ABORT();
    }

    auto typedExpression = OnExpression(itemIndex.front());
    if (typedExpression->GetWireType() != expectedType) {
        THROW_ERROR_EXCEPTION("Incorrect type inside of the list or dict item accessor")
            << TErrorAttribute("source", NAst::FormatReference(reference))
            << TErrorAttribute("actual_type", ToString(expectedType))
            << TErrorAttribute("expected_type", ToString(typedExpression->GetWireType()));
    }

    return typedExpression;
}

TConstExpressionPtr TExprBuilderV2::UnwrapCompositeMemberAccessor(
    const NAst::TReference& reference,
    const TLogicalTypePtr& type)
{

    auto columnReference = New<TReferenceExpression>(type, InferColumnName(reference));

    if (reference.CompositeTypeAccessor.IsEmpty()) {
        return columnReference;
    }

    auto resolved = ResolveNestedTypes(type, reference);
    auto listOrDictItemAccessor = UnwrapListOrDictItemAccessor(reference, resolved.IntermediateType->GetMetatype());

    auto memberAccessor = New<TCompositeMemberAccessorExpression>(
        resolved.ResultType,
        columnReference,
        std::move(resolved.NestedStructOrTupleItemAccessor),
        listOrDictItemAccessor);

    return memberAccessor;
}

TConstExpressionPtr TExprBuilderV2::OnReference(const NAst::TReference& reference)
{
    if (AfterGroupBy_) {
        if (auto type = GetColumnType(reference)) {
            return UnwrapCompositeMemberAccessor(reference, type);
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
        if (auto type = GetColumnType(reference)) {
            return UnwrapCompositeMemberAccessor(reference, type);
        }
    }

    THROW_ERROR_EXCEPTION("Undefined reference %Qv",
        InferColumnName(reference));
}

TConstExpressionPtr TExprBuilderV2::OnFunction(const NAst::TFunctionExpression* functionExpr)
{
    auto functionName = functionExpr->FunctionName;
    functionName.to_lower();

    const auto& descriptor = Functions_->GetFunction(functionName);

    std::vector<TConstExpressionPtr> typedOperands;
    std::vector<TLogicalTypePtr> argumentTypes;

    auto getArguments = [&] {
        typedOperands.reserve(functionExpr->Arguments.size());
        argumentTypes.reserve(functionExpr->Arguments.size());
        for (const auto& argument : functionExpr->Arguments) {
            auto typedArgument = OnExpression(argument);
            argumentTypes.push_back(typedArgument->LogicalType);
            typedOperands.push_back(std::move(typedArgument));
        }
    };

    if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
        getArguments();

        auto functionTypes = regularFunction->InferTypes(
            &TypingCtx_,
            argumentTypes,
            functionName);

        for (int i = 0; i < std::ssize(typedOperands); ++i) {
            if (functionTypes[i + 1] != TypingCtx_.GetTypeId(typedOperands[i]->GetWireType())) {
                auto type = TypingCtx_.GetWireType(functionTypes[i + 1]);
                typedOperands[i] = CreateCoercion(type, typedOperands[i]);
            }
        }

        return New<TFunctionExpression>(TypingCtx_.GetWireType(functionTypes[0]), functionName, typedOperands);
    } else if (const auto* aggregateFunction = descriptor->As<TAggregateFunctionTypeInferrer>()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        if (!AfterGroupBy_) {
            THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName);
        }

        YT_VERIFY(AfterGroupBy_);

        // TODO(lukyan): Use guard.
        AfterGroupBy_ = false;
        getArguments();
        AfterGroupBy_ = true;
        std::vector<TTypeId> inferredTypes;

        inferredTypes = aggregateFunction->InferTypes(
            &TypingCtx_,
            argumentTypes,
            functionName);

        for (int i = 0; i < std::ssize(typedOperands); ++i) {
            if (inferredTypes[i + 2] != TypingCtx_.GetTypeId(typedOperands[i]->GetWireType())) {
                auto type = TypingCtx_.GetWireType(inferredTypes[i + 2]);
                typedOperands[i] = CreateCoercion(type, typedOperands[i]);
            }
        }

        auto resultType = TypingCtx_.GetLogicalType(inferredTypes[0]);
        auto stateType = TypingCtx_.GetLogicalType(inferredTypes[1]);

        auto found = AggregateLookup_.find(subexpressionName);
        if (found != AggregateLookup_.end()) {
            return found->second;
        } else {
            AggregateItems_->emplace_back(
                typedOperands,
                functionName,
                subexpressionName,
                stateType,
                resultType);

            auto expr = New<TReferenceExpression>(
                resultType,
                subexpressionName);
            YT_VERIFY(AggregateLookup_.emplace(subexpressionName, expr).second);
            return expr;
        }
    } else {
        YT_ABORT();
    }
}

TConstExpressionPtr TExprBuilderV2::OnUnaryOp(const NAst::TUnaryOpExpression* unaryExpr)
{
    if (unaryExpr->Operand.size() != 1) {
        THROW_ERROR_EXCEPTION(
            "Unary operator %Qv must have exactly one argument",
            unaryExpr->Opcode);
    }

    auto operand = OnExpression(unaryExpr->Operand.front());

    std::vector<TTypeId> types;

    try {
        types = TypingCtx_.InferFunctionType(
            GetUnaryOpcodeLexeme(unaryExpr->Opcode),
            {TypingCtx_.GetTypeId(operand->GetWireType())});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv",
            GetUnaryOpcodeLexeme(unaryExpr->Opcode))
            << TErrorAttribute("source", unaryExpr->Operand.front()->GetSource(Source_))
            << ex;;
    }

    YT_VERIFY(std::ssize(types) == 2);
    auto type = TypingCtx_.GetWireType(types[0]);

    if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, operand)) {
        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(*foldedExpr, type));
    }

    if (types[1] != TypingCtx_.GetTypeId(operand->GetWireType())) {
        operand = CreateCoercion(TypingCtx_.GetWireType(types[1]), operand);
    }

    return New<TUnaryOpExpression>(
        type,
        unaryExpr->Opcode,
        operand);
}

TConstExpressionPtr TExprBuilderV2::MakeBinaryExpr(
    const NAst::TBinaryOpExpression* binaryExpr,
    EBinaryOp op,
    TConstExpressionPtr typedLhs,
    TConstExpressionPtr typedRhs,
    std::optional<size_t> offset)
{
    auto lhsSource = offset ? binaryExpr->Lhs[*offset]->GetSource(Source_) : "";
    auto rhsSource = offset ? binaryExpr->Rhs[*offset]->GetSource(Source_) : "";

    // TODO(lukyan): Use LogicalType and remove GetWireType.
    // Currently there are issues with defined function signatures.
    // Functions are defined in terms of EValueType. Need signatures for int32, int64...
    // TODO(lukyan): Consider coercions of references in range inference.

    auto lhsTypeId = TypingCtx_.GetTypeId(typedLhs->GetWireType());
    auto rhsTypeId = TypingCtx_.GetTypeId(typedRhs->GetWireType());

    std::vector<TTypeId> types;
    try {
        types = TypingCtx_.InferFunctionType(
            GetBinaryOpcodeLexeme(op),
            {
                lhsTypeId,
                rhsTypeId});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv",
            GetBinaryOpcodeLexeme(op))
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource)
            << TErrorAttribute("lhs_type", TypingCtx_.GetLogicalType(lhsTypeId))
            << TErrorAttribute("rhs_type", TypingCtx_.GetLogicalType(rhsTypeId))
            << ex;
    }

    YT_VERIFY(std::ssize(types) == 3);
    auto type = TypingCtx_.GetWireType(types[0]);

    if (auto foldedExpr = FoldConstants(op, typedLhs, typedRhs)) {
        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(*foldedExpr, type));
    }

    if (types[1] != TypingCtx_.GetTypeId(typedLhs->GetWireType())) {
        typedLhs = CreateCoercion(TypingCtx_.GetWireType(types[1]), typedLhs);
    }

    if (types[2] != TypingCtx_.GetTypeId(typedRhs->GetWireType())) {
        typedRhs = CreateCoercion(TypingCtx_.GetWireType(types[2]), typedRhs);
    }

    return New<TBinaryOpExpression>(
        type,
        op,
        typedLhs,
        typedRhs);
}

struct TBinaryOpGeneratorV2
{
    TExprBuilderV2& Builder;
    const NAst::TBinaryOpExpression* BinaryExpr;

    TConstExpressionPtr Do(size_t keySize, EBinaryOp op)
    {
        YT_VERIFY(keySize > 0);
        size_t offset = keySize - 1;

        auto typedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
        auto typedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

        auto result = Builder.MakeBinaryExpr(BinaryExpr, op, std::move(typedLhs), std::move(typedRhs), offset);

        while (offset > 0) {
            --offset;
            auto typedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
            auto typedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

            auto eq = Builder.MakeBinaryExpr(
                BinaryExpr,
                op == EBinaryOp::NotEqual ? EBinaryOp::Or : EBinaryOp::And,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    op == EBinaryOp::NotEqual ? EBinaryOp::NotEqual : EBinaryOp::Equal,
                    typedLhs,
                    typedRhs,
                    offset),
                std::move(result),
                std::nullopt);

            if (op == EBinaryOp::Equal || op == EBinaryOp::NotEqual) {
                result = eq;
                continue;
            }

            EBinaryOp strictOp = op;
            if (op == EBinaryOp::LessOrEqual) {
                strictOp = EBinaryOp::Less;
            } else if (op == EBinaryOp::GreaterOrEqual)  {
                strictOp = EBinaryOp::Greater;
            }

            result = Builder.MakeBinaryExpr(
                BinaryExpr,
                EBinaryOp::Or,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    strictOp,
                    std::move(typedLhs),
                    std::move(typedRhs),
                    offset),
                std::move(eq),
                std::nullopt);
        }

        return result;
    }
};

TConstExpressionPtr TExprBuilderV2::OnBinaryOp(
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
        return TBinaryOpGeneratorV2{*this, binaryExpr}.Do(keySize, binaryExpr->Opcode);
    } else {
        if (binaryExpr->Lhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Lhs));
        }

        if (binaryExpr->Rhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", FormatExpression(binaryExpr->Rhs));
        }

        auto typedLhs = OnExpression(binaryExpr->Lhs.front());
        auto typedRhs = OnExpression(binaryExpr->Rhs.front());

        return MakeBinaryExpr(binaryExpr, binaryExpr->Opcode, std::move(typedLhs), std::move(typedRhs), 0);
    }
}

void TExprBuilderV2::InferArgumentTypes(
    std::vector<TConstExpressionPtr>* typedArguments,
    std::vector<EValueType>* argTypes,
    const NAst::TExpressionList& expressions,
    TStringBuf operatorName,
    TStringBuf source)
{
    std::unordered_set<std::string> columnNames;

    for (const auto& argument : expressions) {
        auto typedArgument = BuildTypedExpression(argument);

        if (auto reference = typedArgument->As<TReferenceExpression>()) {
            if (!columnNames.insert(reference->ColumnName).second) {
                THROW_ERROR_EXCEPTION("%v operator has multiple references to column %Qv",
                    operatorName,
                    reference->ColumnName)
                    << TErrorAttribute("source", source);
            }
        }

        argTypes->push_back(typedArgument->GetWireType());
        typedArguments->push_back(std::move(typedArgument));
    }
}

TConstExpressionPtr TExprBuilderV2::OnInOp(
    const NAst::TInExpression* inExpr)
{
    auto source = inExpr->GetSource(Source_);

    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

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
    return New<TInExpression>(std::move(typedArguments), std::move(capturedRows));
}

TConstExpressionPtr TExprBuilderV2::OnBetweenOp(
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
    return New<TBetweenExpression>(std::move(typedArguments), std::move(capturedRows));
}

TConstExpressionPtr TExprBuilderV2::OnTransformOp(
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

    std::vector<TTypeId> resultTypes;

    for (const auto& tuple : transformExpr->To) {
        if (tuple.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                << TErrorAttribute("source", source);
        }

        resultTypes.push_back(TypingCtx_.GetTypeId(GetType(tuple.front())));
    }

    TConstExpressionPtr defaultTypedExpr;

    if (const auto& defaultExpr = transformExpr->DefaultExpr) {
        if (defaultExpr->size() != 1) {
            THROW_ERROR_EXCEPTION("Default expression must scalar")
                << TErrorAttribute("source", source);
        }

        defaultTypedExpr = OnExpression(defaultExpr->front());
        resultTypes.push_back(TypingCtx_.GetTypeId(defaultTypedExpr->GetWireType()));
    }

    std::sort(resultTypes.begin(), resultTypes.end());
    resultTypes.erase(std::unique(resultTypes.begin(), resultTypes.end()), resultTypes.end());

    // TODO(lukyan): On error find first occurence of value with no coercion.
    auto resultType = TypingCtx_.GetWireType(UnifyTypes("transform", &TypingCtx_, resultTypes));

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
    return New<TTransformExpression>(
        resultType,
        std::move(typedArguments),
        std::move(capturedRows),
        std::move(defaultTypedExpr));
}

TConstExpressionPtr TExprBuilderV2::OnCaseOp(const NAst::TCaseExpression* caseExpr)
{
    auto source = caseExpr->GetSource(Source_);

    TConstExpressionPtr typedOptionalOperand;
    if (caseExpr->OptionalOperand) {
        if (caseExpr->OptionalOperand->size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE should be scalar")
                << TErrorAttribute("source", source);
        }

        typedOptionalOperand = OnExpression(caseExpr->OptionalOperand->front());
    }

    std::vector<TWhenThenExpressionPtr> typedWhenThenExpressions;
    typedWhenThenExpressions.reserve(caseExpr->WhenThenExpressions.size());

    auto expectedConditionType = TypingCtx_.GetTypeId(typedOptionalOperand
        ? typedOptionalOperand->GetWireType()
        : EValueType::Boolean);

    std::vector<TTypeId> resultTypes;

    for (const auto& [when, then] : caseExpr->WhenThenExpressions) {
        if (when.size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be scalar")
                << TErrorAttribute("source", source);
        }

        auto whenExpression = OnExpression(when.front());
        auto actualConditionType = TypingCtx_.GetTypeId(whenExpression->GetWireType());

        try {
            // Ignore result, only check.
            TypingCtx_.InferFunctionType(
                "when",
                {TTypingCtx::TFunctionSignature({expectedConditionType})},
                {actualConditionType},
                0);

        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Types mismatch in CASE WHEN expression")
                << ex;
        }

        if (then.size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE THEN should be scalar")
                << TErrorAttribute("source", source);
        }

        if (actualConditionType != expectedConditionType) {
            whenExpression = CreateCoercion(TypingCtx_.GetWireType(expectedConditionType), whenExpression);
        }

        auto thenExpression = OnExpression(then.front());

        resultTypes.push_back(TypingCtx_.GetTypeId(thenExpression->GetWireType()));

        typedWhenThenExpressions.push_back(
            New<TWhenThenExpression>(std::move(whenExpression), std::move(thenExpression)));
    }

    TConstExpressionPtr typedDefaultExpression;
    if (caseExpr->DefaultExpression) {
        if (caseExpr->DefaultExpression->size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE ELSE should be scalar")
                << TErrorAttribute("source", source);
        }

        typedDefaultExpression = OnExpression(caseExpr->DefaultExpression->front());

        resultTypes.push_back(TypingCtx_.GetTypeId(typedDefaultExpression->GetWireType()));
    }

    std::sort(resultTypes.begin(), resultTypes.end());
    resultTypes.erase(std::unique(resultTypes.begin(), resultTypes.end()), resultTypes.end());

    // TODO(lukyan): On error find first occurence of value with no coercion.
    TTypeId resultType;
    try {
        resultType = UnifyTypes("case", &TypingCtx_, resultTypes);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN/ELSE expression")
            << ex;
    }

    for (auto typedWhenThen : typedWhenThenExpressions) {
        auto thenResultType = TypingCtx_.GetTypeId(typedWhenThen->Result->GetWireType());

        if (thenResultType != resultType) {
            typedWhenThen->Result = CreateCoercion(TypingCtx_.GetWireType(resultType), typedWhenThen->Result);
        }
    }

    return New<TCaseExpression>(
        TypingCtx_.GetWireType(resultType),
        std::move(typedOptionalOperand),
        std::move(typedWhenThenExpressions),
        std::move(typedDefaultExpression));
}

TConstExpressionPtr TExprBuilderV2::OnLikeOp(const NAst::TLikeExpression* likeExpr)
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
        if (expression.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar %Qv expression",
                name)
                << TErrorAttribute("source", source);
        }

        auto typedExpression = OnExpression(expression.front());

        if (typedExpression->GetWireType() != EValueType::String && typedExpression->GetWireType() != EValueType::Null) {
            THROW_ERROR_EXCEPTION("Types mismatch in %v", name)
                << TErrorAttribute("source", source)
                << TErrorAttribute("actual_type", typedExpression->GetWireType())
                << TErrorAttribute("expected_type", EValueType::String);
        }

        return typedExpression;
    };

    auto typedText = makeTypedStringExpression(likeExpr->Text, "LIKE matched value", source);
    auto typedPattern = makeTypedStringExpression(likeExpr->Pattern, "LIKE pattern", source);

    TConstExpressionPtr typedEscapeCharacter;
    if (likeExpr->EscapeCharacter) {
        typedEscapeCharacter = makeTypedStringExpression(likeExpr->EscapeCharacter.value(), "escape character", source);
    }

    return New<TLikeExpression>(
        std::move(typedText),
        likeExpr->Opcode,
        std::move(typedPattern),
        std::move(typedEscapeCharacter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
