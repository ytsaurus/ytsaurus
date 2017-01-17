#include "query_preparer.h"
#include "private.h"
#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "plan_helpers.h"

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/core/misc/common.h>

#include <yt/core/ytree/yson_serializable.h>

#include <unordered_set>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

struct TQueryPreparerBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

typedef std::pair<NAst::TQuery, NAst::TAliasMap> TParsedQueryInfo;

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<Stroka>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<Stroka>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(to_lower(functionExpr->FunctionName));
        ExtractFunctionNames(functionExpr->Arguments, functions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        ExtractFunctionNames(unaryExpr->Operand, functions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        ExtractFunctionNames(binaryExpr->Lhs, functions);
        ExtractFunctionNames(binaryExpr->Rhs, functions);
    } else if (expr->As<NAst::TInExpression>()) {
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else {
        YUNREACHABLE();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<Stroka>* functions)
{
    if (!exprs) {
        return;
    }

    for (const auto& expr : exprs.Get()) {
        ExtractFunctionNames(expr, functions);
    }
}

std::vector<Stroka> ExtractFunctionNames(
    const TParsedQueryInfo& parsedQueryInfo)
{
    std::vector<Stroka> functions;

    ExtractFunctionNames(parsedQueryInfo.first.WherePredicate, &functions);
    ExtractFunctionNames(parsedQueryInfo.first.HavingPredicate, &functions);
    ExtractFunctionNames(parsedQueryInfo.first.SelectExprs, &functions);

    if (auto groupExprs = parsedQueryInfo.first.GroupExprs.GetPtr()) {
        for (const auto& expr : groupExprs->first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& join : parsedQueryInfo.first.Joins) {
        ExtractFunctionNames(join.Left, &functions);
        ExtractFunctionNames(join.Right, &functions);
    }

    for (const auto& orderExpression : parsedQueryInfo.first.OrderExpressions) {
        for (const auto& expr : orderExpression.first) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& aliasedExpression : parsedQueryInfo.second) {
        ExtractFunctionNames(aliasedExpression.second.Get(), &functions);
    }

    std::sort(functions.begin(), functions.end());
    functions.erase(
        std::unique(functions.begin(), functions.end()),
        functions.end());

    return functions;
}

////////////////////////////////////////////////////////////////////////////////

void CheckExpressionDepth(TConstExpressionPtr op, int depth = 0)
{
    if (depth > PlanFragmentDepthLimit) {
        THROW_ERROR_EXCEPTION("Plan fragment depth limit exceeded");
    }

    if (op->As<TLiteralExpression>() || op->As<TReferenceExpression>() || op->As<TInOpExpression>()) {
        return;
    } else if (auto functionExpr = op->As<TFunctionExpression>()) {
        for (const auto& argument : functionExpr->Arguments) {
            CheckExpressionDepth(argument, depth + 1);
        }
        return;
    } else if (auto unaryOpExpr = op->As<TUnaryOpExpression>()) {
        CheckExpressionDepth(unaryOpExpr->Operand, depth + 1);
        return;
    } else if (auto binaryOpExpr = op->As<TBinaryOpExpression>()) {
        CheckExpressionDepth(binaryOpExpr->Lhs, depth + 1);
        CheckExpressionDepth(binaryOpExpr->Rhs, depth + 1);
        return;
    }
    YUNREACHABLE();
};

EValueType InferUnaryExprType(EUnaryOp opCode, EValueType operandType, const TStringBuf& source)
{
    switch (opCode) {
        case EUnaryOp::Plus:
        case EUnaryOp::Minus:
            if (!IsArithmeticType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral or floating-point operand",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EUnaryOp::BitNot:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operand",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EUnaryOp::Not:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operand",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        default:
            YUNREACHABLE();
    }
}

EValueType InferBinaryExprType(
    EBinaryOp opCode,
    EValueType lhsType,
    EValueType rhsType,
    const TStringBuf& source,
    const TStringBuf& lhsSource,
    const TStringBuf& rhsSource)
{
    if (lhsType != rhsType) {
        THROW_ERROR_EXCEPTION(
            "Type mismatch in expression %Qv",
            source)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource)
            << TErrorAttribute("lhs_type", lhsType)
            << TErrorAttribute("rhs_type", rhsType);
    }

    EValueType operandType = lhsType;

    switch (opCode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            if (!IsArithmeticType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral or floating-point operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
            }
            return operandType;

        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
            }
            return operandType;

        case EBinaryOp::And:
        case EBinaryOp::Or:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("operand_type", operandType);
            }
            return EValueType::Boolean;

        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::Greater:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::GreaterOrEqual:
            if (!IsComparableType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral, floating-point or string operands",
                    source)
                    << TErrorAttribute("lhs_source", lhsSource)
                    << TErrorAttribute("rhs_source", rhsSource)
                    << TErrorAttribute("lhs_type", operandType);
            }
            return EValueType::Boolean;

        default:
            YUNREACHABLE();
    }
}

DECLARE_REFCOUNTED_CLASS(TSchemaProxy)

class TSchemaProxy
    : public TIntrinsicRefCounted
{
public:
    explicit TSchemaProxy(
        TTableSchema* tableSchema,
        const TStringBuf& tableName = TStringBuf())
        : TableSchema_(tableSchema)
    {
        YCHECK(tableSchema);
        const auto& columns = TableSchema_->Columns();
        for (size_t index = 0; index < columns.size(); ++index) {
            Lookup_.insert(std::make_pair(std::make_pair(Stroka(columns[index].Name), Stroka(tableName)), index));
        }
    }

    // NOTE: result must be used before next call
    const TColumnSchema* GetColumnPtr(const TStringBuf& name, const TStringBuf& tableName)
    {
        const auto& resultColumns = TableSchema_->Columns();
        auto it = Lookup_.find(std::make_pair(Stroka(name), Stroka(tableName)));
        if (it != Lookup_.end()) {
            return &resultColumns[it->second];
        } else if (auto original = AddColumnPtr(name, tableName)) {
            auto index = resultColumns.size();
            Lookup_.insert(std::make_pair(std::make_pair(Stroka(name), Stroka(tableName)), index));
            TColumnSchema newColumn(NAst::FormatColumn(name, tableName), original->Type);
            TableSchema_->AppendColumn(newColumn);
            return &TableSchema_->Columns().back();
        }
        return nullptr;
    }

    const yhash_map<std::pair<Stroka, Stroka>, size_t>& GetLookup() const
    {
        return Lookup_;
    }

    const TColumnSchema* GetAggregateColumnPtr(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap)
    {
        auto& resultColumns = TableSchema_->Columns();
        auto it = Lookup_.find(std::make_pair(Stroka(subexprName), Stroka()));
        if (it != Lookup_.end()) {
            return &resultColumns[it->second];
        }

        auto original = AddAggregateColumnPtr(
            name,
            aggregateFunction,
            arguments,
            subexprName,
            source,
            functions,
            aliasMap);

        auto index = resultColumns.size();
        Lookup_.insert(std::make_pair(std::make_pair(Stroka(subexprName), Stroka()), index));
        TableSchema_->AppendColumn(original);
        return &TableSchema_->Columns().back();
    }

    virtual void Finish()
    { }

    struct TNamedExpression
    {
        TConstExpressionPtr Expr;
        TNullable<Stroka> Name;

        TNamedExpression(const TConstExpressionPtr& expr, const TNullable<Stroka>& name = TNullable<Stroka>())
            : Expr(expr)
            , Name(name)
        { }
    };

    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap,
        yhash_set<Stroka>* references = nullptr)
    {
        std::set<Stroka> usedAliases;
        return PropagateNotExpression(
            DoBuildTypedExpression(expr, source, functions, aliasMap, usedAliases, references));
    }

    DEFINE_BYVAL_RO_PROPERTY(TTableSchema*, TableSchema);

protected:
    TConstExpressionPtr DoBuildTypedExpression(
        const NAst::TExpression* expr,
        const Stroka& source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap,
        std::set<Stroka>& usedAliases,
        yhash_set<Stroka>* references)
    {
        if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            const auto& literalValue = literalExpr->Value;

            return New<TLiteralExpression>(
                GetType(literalValue),
                GetValue(literalValue));
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            const auto* column = GetColumnPtr(referenceExpr->ColumnName, referenceExpr->TableName);
            if (!column) {
                if (referenceExpr->TableName.empty()) {
                    auto columnName = referenceExpr->ColumnName;
                    auto found = aliasMap.find(columnName);

                    if (found != aliasMap.end()) {
                        if (usedAliases.count(columnName)) {
                            THROW_ERROR_EXCEPTION("Recursive usage of alias %Qv", columnName);
                        }

                        usedAliases.insert(columnName);
                        auto aliasExpr = DoBuildTypedExpression(
                            found->second.Get(),
                            source,
                            functions,
                            aliasMap,
                            usedAliases,
                            references);

                        usedAliases.erase(columnName);
                        return aliasExpr;
                    }
                }

                THROW_ERROR_EXCEPTION("Undefined reference %Qv",
                    NAst::FormatColumn(referenceExpr->ColumnName, referenceExpr->TableName));
            }

            if (references) {
                references->insert(column->Name);
            }

            return New<TReferenceExpression>(column->Type, column->Name);
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;
            functionName.to_lower();

            const auto& descriptor = functions->GetFunction(functionName);

            if (const auto* aggregateFunction = descriptor->As<TAggregateTypeInferrer>()) {
                auto subexprName = InferName(functionExpr);

                try {
                    if (functionExpr->Arguments.size() != 1) {
                        THROW_ERROR_EXCEPTION(
                            "Aggregate function %Qv must have exactly one argument",
                            functionName);
                    }

                    const auto* aggregateColumn = GetAggregateColumnPtr(
                        functionName,
                        aggregateFunction,
                        functionExpr->Arguments.front().Get(),
                        subexprName,
                        source,
                        functions,
                        aliasMap);

                    return New<TReferenceExpression>(
                        aggregateColumn->Type,
                        aggregateColumn->Name);

                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error creating aggregate")
                        << TErrorAttribute("source", functionExpr->GetSource(source))
                        << ex;
                }
            } else if (const auto* regularFunction = descriptor->As<TFunctionTypeInferrer>()) {
                std::vector<EValueType> types;
                std::vector<TConstExpressionPtr> typedOperands;
                for (const auto& argument : functionExpr->Arguments) {
                    auto typedArgument = DoBuildTypedExpression(
                        argument.Get(),
                        source,
                        functions,
                        aliasMap,
                        usedAliases,
                        references);
                    types.push_back(typedArgument->Type);
                    typedOperands.push_back(typedArgument);
                }

                return New<TFunctionExpression>(
                    regularFunction->InferResultType(types, functionName, functionExpr->GetSource(source)),
                    functionName,
                    typedOperands);
            }
        } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
            if (unaryExpr->Operand.size() != 1) {
                THROW_ERROR_EXCEPTION(
                    "Unary operator %Qv must have exactly one argument",
                    unaryExpr->Opcode);
            }

            auto typedOperand = DoBuildTypedExpression(
                unaryExpr->Operand.front().Get(),
                source,
                functions,
                aliasMap,
                usedAliases,
                references);

            if (auto foldedExpr = FoldConstants(unaryExpr, typedOperand)) {
                return foldedExpr;
            } else {
                return New<TUnaryOpExpression>(
                    InferUnaryExprType(
                        unaryExpr->Opcode,
                        typedOperand->Type,
                        unaryExpr->GetSource(source)),
                    unaryExpr->Opcode,
                    typedOperand);
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto makeBinaryExpr = [&] (EBinaryOp op, TConstExpressionPtr lhs, TConstExpressionPtr rhs) -> TConstExpressionPtr {
                auto type = InferBinaryExprType(
                    op,
                    lhs->Type,
                    rhs->Type,
                    binaryExpr->GetSource(source),
                    InferName(lhs),
                    InferName(rhs));
                if (auto foldedExpr = FoldConstants(binaryExpr, lhs, rhs)) {
                    return foldedExpr;
                } else {
                    return New<TBinaryOpExpression>(type, op, lhs, rhs);
                }
            };

            std::function<TConstExpressionPtr(int, int, EBinaryOp)> gen = [&] (int offset, int keySize, EBinaryOp op) -> TConstExpressionPtr {
                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs[offset].Get(),
                    source,
                    functions,
                    aliasMap,
                    usedAliases,
                    references);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs[offset].Get(),
                    source,
                    functions,
                    aliasMap,
                    usedAliases,
                    references);

                if (offset + 1 < keySize) {
                    auto next = gen(offset + 1, keySize, op);
                    auto eq = MakeAndExpression(
                            makeBinaryExpr(EBinaryOp::Equal, typedLhs, typedRhs),
                            next);
                    if (op == EBinaryOp::Less || op == EBinaryOp::LessOrEqual) {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Less, typedLhs, typedRhs),
                            eq);
                    } else if (op == EBinaryOp::Greater || op == EBinaryOp::GreaterOrEqual)  {
                        return MakeOrExpression(
                            makeBinaryExpr(EBinaryOp::Greater, typedLhs, typedRhs),
                            eq);
                    } else {
                        return eq;
                    }
                } else {
                    return makeBinaryExpr(op, typedLhs, typedRhs);
                }
            };

            if (binaryExpr->Opcode == EBinaryOp::Less
                || binaryExpr->Opcode == EBinaryOp::LessOrEqual
                || binaryExpr->Opcode == EBinaryOp::Greater
                || binaryExpr->Opcode == EBinaryOp::GreaterOrEqual
                || binaryExpr->Opcode == EBinaryOp::Equal) {

                if (binaryExpr->Lhs.size() != binaryExpr->Rhs.size()) {
                    THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                        binaryExpr->Lhs.size(),
                        binaryExpr->Rhs.size())
                        << TErrorAttribute("source", binaryExpr->GetSource(source));
                }

                int keySize = binaryExpr->Lhs.size();
                return gen(0, keySize, binaryExpr->Opcode);
            } else {
                if (binaryExpr->Lhs.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", InferName(binaryExpr->Lhs));
                }

                if (binaryExpr->Rhs.size() != 1) {
                    THROW_ERROR_EXCEPTION("Expecting scalar expression")
                        << TErrorAttribute("source", InferName(binaryExpr->Rhs));
                }

                auto typedLhs = DoBuildTypedExpression(
                    binaryExpr->Lhs.front().Get(),
                    source,
                    functions,
                    aliasMap,
                    usedAliases,
                    references);
                auto typedRhs = DoBuildTypedExpression(
                    binaryExpr->Rhs.front().Get(),
                    source,
                    functions,
                    aliasMap,
                    usedAliases,
                    references);

                return makeBinaryExpr(binaryExpr->Opcode, typedLhs, typedRhs);
            }
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            std::vector<TConstExpressionPtr> typedArguments;
            std::unordered_set<Stroka> columnNames;
            std::vector<EValueType> argTypes;

            for (const auto& argument : inExpr->Expr) {
                auto typedArgument = DoBuildTypedExpression(
                    argument.Get(),
                    source,
                    functions,
                    aliasMap,
                    usedAliases,
                    references);

                typedArguments.push_back(typedArgument);
                argTypes.push_back(typedArgument->Type);
                if (auto reference = typedArgument->As<TReferenceExpression>()) {
                    if (columnNames.find(reference->ColumnName) != columnNames.end()) {
                        THROW_ERROR_EXCEPTION("IN operator has multiple references to column %Qv", reference->ColumnName)
                            << TErrorAttribute("source", source);
                    } else {
                        columnNames.insert(reference->ColumnName);
                    }
                }
            }

            auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, inExpr->GetSource(source));
            return New<TInOpExpression>(std::move(typedArguments), std::move(capturedRows));
        }

        YUNREACHABLE();
    }

    TConstExpressionPtr PropagateNotExpression(TConstExpressionPtr expr)
    {
        if (expr->As<TReferenceExpression>() ||
            expr->As<TLiteralExpression>())
        {
            return expr;
        } else if (auto inExpr = expr->As<TInOpExpression>()) {
            std::vector<TConstExpressionPtr> propagatedArgumenst;
            for (auto argument : inExpr->Arguments) {
                propagatedArgumenst.push_back(PropagateNotExpression(argument));
            }
            return New<TInOpExpression>(
                std::move(propagatedArgumenst),
                inExpr->Values);
        } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
            std::vector<TConstExpressionPtr> propagatedArgumenst;
            for (auto argument : functionExpr->Arguments) {
                propagatedArgumenst.push_back(PropagateNotExpression(argument));
            }
            return New<TFunctionExpression>(
                functionExpr->Type,
                functionExpr->FunctionName,
                std::move(propagatedArgumenst));
        } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
            return New<TBinaryOpExpression>(
                binaryOp->Type,
                binaryOp->Opcode,
                PropagateNotExpression(binaryOp->Lhs),
                PropagateNotExpression(binaryOp->Rhs));
        } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
            auto& operand = unaryOp->Operand;
            if (unaryOp->Opcode == EUnaryOp::Not) {
                if (auto operandUnaryOp = operand->As<TUnaryOpExpression>()) {
                    if (operandUnaryOp->Opcode == EUnaryOp::Not) {
                        return PropagateNotExpression(operandUnaryOp->Operand);
                    }
                } else if (auto operandBinaryOp = operand->As<TBinaryOpExpression>()) {
                    if (operandBinaryOp->Opcode == EBinaryOp::And) {
                        return PropagateNotExpression(MakeOrExpression(
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Lhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Lhs),
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Rhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Rhs)));
                    } else if (operandBinaryOp->Opcode == EBinaryOp::Or) {
                        return PropagateNotExpression(MakeAndExpression(
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Lhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Lhs),
                            New<TUnaryOpExpression>(
                                operandBinaryOp->Rhs->Type,
                                EUnaryOp::Not,
                                operandBinaryOp->Rhs)));
                    } else if (IsRelationalBinaryOp(operandBinaryOp->Opcode)) {
                        return PropagateNotExpression(New<TBinaryOpExpression>(
                            operandBinaryOp->Type,
                            GetInversedBinaryOpcode(operandBinaryOp->Opcode),
                            operandBinaryOp->Lhs,
                            operandBinaryOp->Rhs));
                    }
                } else if (auto literal = operand->As<TLiteralExpression>()) {
                    TUnversionedValue value = literal->Value;
                    value.Data.Boolean = !value.Data.Boolean;
                    return New<TLiteralExpression>(
                        literal->Type,
                        value);
                }
            }
            return New<TUnaryOpExpression>(
                unaryOp->Type,
                unaryOp->Opcode,
                PropagateNotExpression(operand));
        }
        YUNREACHABLE();
    }

private:
    yhash_map<std::pair<Stroka, Stroka>, size_t> Lookup_;

protected:
    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName)
    {
        return nullptr;
    }

    // NOTE: result must be used before next call
    virtual TColumnSchema AddAggregateColumnPtr(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* arguments,
        Stroka subexprName,
        Stroka source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap)
    {
        THROW_ERROR_EXCEPTION(
            "Misuse of aggregate function %v",
            name);
    }

    static EValueType GetType(const NAst::TLiteralValue& literalValue)
    {
        switch (literalValue.Tag()) {
            case NAst::TLiteralValue::TagOf<i64>():
                return EValueType::Int64;
            case NAst::TLiteralValue::TagOf<ui64>():
                return EValueType::Uint64;
            case NAst::TLiteralValue::TagOf<double>():
                return EValueType::Double;
            case NAst::TLiteralValue::TagOf<bool>():
                return EValueType::Boolean;
            case NAst::TLiteralValue::TagOf<Stroka>():
                return EValueType::String;
            default:
                YUNREACHABLE();
        }
    }

    static TValue GetValue(const NAst::TLiteralValue& literalValue)
    {
        switch (literalValue.Tag()) {
            case NAst::TLiteralValue::TagOf<i64>():
                return MakeUnversionedInt64Value(literalValue.As<i64>());
            case NAst::TLiteralValue::TagOf<ui64>():
                return MakeUnversionedUint64Value(literalValue.As<ui64>());
            case NAst::TLiteralValue::TagOf<double>():
                return MakeUnversionedDoubleValue(literalValue.As<double>());
            case NAst::TLiteralValue::TagOf<bool>():
                return MakeUnversionedBooleanValue(literalValue.As<bool>());
            case NAst::TLiteralValue::TagOf<Stroka>():
                return MakeUnversionedStringValue(
                    literalValue.As<Stroka>().c_str(),
                    literalValue.As<Stroka>().length());
            default:
                YUNREACHABLE();
        }
    }

    static TSharedRange<TRow> LiteralTupleListToRows(
        const NAst::TLiteralValueTupleList& literalTuples,
        const std::vector<EValueType>& argTypes,
        const TStringBuf& source)
    {
        auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
        TUnversionedRowBuilder rowBuilder;
        std::vector<TRow> rows;
        for (const auto& tuple : literalTuples) {
            if (tuple.size() != argTypes.size()) {
                THROW_ERROR_EXCEPTION("IN operator arguments size mismatch")
                    << TErrorAttribute("source", source);
            }
            for (int i = 0; i < tuple.size(); ++i) {
                auto valueType = GetType(tuple[i]);
                if (valueType != argTypes[i]) {
                    THROW_ERROR_EXCEPTION("IN operator types mismatch")
                        << TErrorAttribute("source", source)
                        << TErrorAttribute("actual_type", valueType)
                        << TErrorAttribute("expected_type", argTypes[i]);
                }
                rowBuilder.AddValue(GetValue(tuple[i]));
            }
            rows.push_back(rowBuffer->Capture(rowBuilder.GetRow()));
            rowBuilder.Reset();
        }

        std::sort(rows.begin(), rows.end());
        return MakeSharedRange(std::move(rows), std::move(rowBuffer));
    }

    TConstExpressionPtr FoldConstants(
        const NAst::TUnaryOpExpression* unaryExpr,
        TConstExpressionPtr operand)
    {
        auto foldConstants = [] (EUnaryOp opcode, TConstExpressionPtr operand) -> TNullable<TUnversionedValue> {
            if (auto literalExpr = operand->As<TLiteralExpression>()) {
                if (opcode == EUnaryOp::Plus) {
                    return static_cast<TUnversionedValue>(literalExpr->Value);
                } else if (opcode == EUnaryOp::Minus) {
                    TUnversionedValue value = literalExpr->Value;
                    switch (value.Type) {
                        case EValueType::Int64:
                            value.Data.Int64 = -value.Data.Int64;
                            break;
                        case EValueType::Uint64:
                            value.Data.Uint64 = -value.Data.Uint64;
                            break;
                        case EValueType::Double:
                            value.Data.Double = -value.Data.Double;
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    return value;
                } else if (opcode == EUnaryOp::BitNot) {
                    TUnversionedValue value = literalExpr->Value;
                    switch (value.Type) {
                        case EValueType::Int64:
                            value.Data.Int64 = ~value.Data.Int64;
                            break;
                        case EValueType::Uint64:
                            value.Data.Uint64 = ~value.Data.Uint64;
                            break;
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    return value;
                }
            }
            return TNullable<TUnversionedValue>();
        };

        if (auto value = foldConstants(unaryExpr->Opcode, operand)) {
            return New<TLiteralExpression>(value->Type, *value);
        }

        return TConstExpressionPtr();
    }

    TConstExpressionPtr FoldConstants(
        const NAst::TBinaryOpExpression* binaryExpr,
        TConstExpressionPtr lhsExpr,
        TConstExpressionPtr rhsExpr)
    {
        auto foldConstants = [] (
            EBinaryOp opcode,
            TConstExpressionPtr lhsExpr,
            TConstExpressionPtr rhsExpr)
            -> TNullable<TUnversionedValue>
        {
            auto lhsLiteral = lhsExpr->As<TLiteralExpression>();
            auto rhsLiteral = rhsExpr->As<TLiteralExpression>();
            if (lhsLiteral && rhsLiteral) {
                auto lhs = static_cast<TUnversionedValue>(lhsLiteral->Value);
                auto rhs = static_cast<TUnversionedValue>(rhsLiteral->Value);
                YCHECK(lhs.Type == rhs.Type);

                switch (opcode) {
                    case EBinaryOp::Plus:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 += rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 += rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double += rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Minus:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 -= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 -= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double -= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Multiply:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 *= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 *= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                lhs.Data.Double *= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Divide:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                if (rhs.Data.Int64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Int64 /= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                if (rhs.Data.Uint64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Uint64 /= rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Double:
                                if (std::abs(rhs.Data.Double) <= std::numeric_limits<double>::epsilon()) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Double /= rhs.Data.Double;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Modulo:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                if (rhs.Data.Int64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Int64 %= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                if (rhs.Data.Uint64 == 0) {
                                    THROW_ERROR_EXCEPTION("Division by zero");
                                }
                                lhs.Data.Uint64 %= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::LeftShift:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 <<= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 <<= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::RightShift:
                        switch (lhs.Type) {
                            case EValueType::Int64:
                                lhs.Data.Int64 >>= rhs.Data.Int64;
                                return lhs;
                            case EValueType::Uint64:
                                lhs.Data.Uint64 >>= rhs.Data.Uint64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::BitOr:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 | rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 | rhs.Data.Int64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::BitAnd:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 & rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 & rhs.Data.Int64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::And:
                        switch (lhs.Type) {
                            case EValueType::Uint64:
                                lhs.Data.Uint64 = lhs.Data.Uint64 & rhs.Data.Uint64;
                                return lhs;
                            case EValueType::Int64:
                                lhs.Data.Int64 = lhs.Data.Int64 & rhs.Data.Int64;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Or:
                        switch (lhs.Type) {
                            case EValueType::Boolean:
                                lhs.Data.Boolean = lhs.Data.Boolean || rhs.Data.Boolean;
                                return lhs;
                            default:
                                break;
                        }
                        break;
                    case EBinaryOp::Equal:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) == 0);
                        break;
                    case EBinaryOp::NotEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) != 0);
                        break;
                    case EBinaryOp::Less:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) < 0);
                        break;
                    case EBinaryOp::Greater:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) > 0);
                        break;
                    case EBinaryOp::LessOrEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) <= 0);
                        break;
                    case EBinaryOp::GreaterOrEqual:
                        return MakeUnversionedBooleanValue(CompareRowValues(lhs, rhs) >= 0);
                        break;
                    default:
                        break;
                }
            }
            return TNullable<TUnversionedValue>();
        };

        if (auto value = foldConstants(binaryExpr->Opcode, lhsExpr, rhsExpr)) {
            return New<TLiteralExpression>(value->Type, *value);
        }

        if (binaryExpr->Opcode == EBinaryOp::Divide) {
            auto lhsBinaryExpr = lhsExpr->As<TBinaryOpExpression>();
            auto rhsLiteralExpr = rhsExpr->As<TLiteralExpression>();
            if (lhsBinaryExpr && rhsLiteralExpr && lhsBinaryExpr->Opcode == EBinaryOp::Divide) {
                auto lhsLiteralExpr = lhsBinaryExpr->Rhs->As<TLiteralExpression>();
                if (lhsLiteralExpr) {
                    TUnversionedValue lhs = lhsLiteralExpr->Value;
                    TUnversionedValue rhs = rhsLiteralExpr->Value;
                    YCHECK(lhs.Type == rhs.Type);

                    auto overflow = [] (ui64 a, ui64 b, bool isSigned) {
                        auto rsh = [] (ui64 base, int amount) {return base >> amount;};
                        auto lower = [] (ui64 base) {return base & 0xffffffff;};
                        ui64 a1 = lower(a);
                        ui64 a2 = rsh(a, 32);
                        ui64 b1 = lower(b);
                        ui64 b2 = rsh(b, 32);
                        int shift = isSigned ? 31 : 32;
                        return (a2 * b2 != 0 ||
                            rsh(a1 * b2, shift) != 0 ||
                            rsh(a2 * b1, shift) != 0 ||
                            rsh(lower(a1 * b2) + lower(a2 * b1) + rsh(a1 * b1, 32), shift) != 0);
                    };

                    auto makeBinaryExpr = [&] (TUnversionedValue divisor) {
                        return New<TBinaryOpExpression>(
                            divisor.Type,
                            EBinaryOp::Divide,
                            lhsBinaryExpr->Lhs,
                            New<TLiteralExpression>(
                                divisor.Type,
                                divisor));
                    };

                    switch (lhs.Type) {
                        case EValueType::Int64:
                            if (!overflow(lhs.Data.Int64, rhs.Data.Int64, true)) {
                                lhs.Data.Int64 *= rhs.Data.Int64;
                                return makeBinaryExpr(lhs);
                            }
                            break;
                        case EValueType::Uint64:
                            if (!overflow(lhs.Data.Uint64, rhs.Data.Uint64, false)) {
                                lhs.Data.Uint64 *= rhs.Data.Uint64;
                                return makeBinaryExpr(lhs);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        return TConstExpressionPtr();
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemaProxy)

class TScanSchemaProxy
    : public TSchemaProxy
{
public:
    static TSchemaProxyPtr Create(
        TTableSchema* tableSchema,
        TTableSchema* refinedTableSchema,
        const TTableSchema& sourceTableSchema,
        int keyColumnCount = 0,
        const TStringBuf& tableName = TStringBuf())
    {
        auto schemaProxy = New<TScanSchemaProxy>(
            tableSchema,
            refinedTableSchema,
            sourceTableSchema,
            tableName);
        schemaProxy->Init(keyColumnCount);
        return schemaProxy;
    }

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        if (tableName != TableName_) {
            return nullptr;
        }

        auto column = RefinedTableSchema_->FindColumn(name);
        if (column) {
            return column;
        }

        column = SourceTableSchema_.FindColumn(name);
        if (column) {
            RefinedTableSchema_->AppendColumn(*column);
        }
        return column;
    }

    virtual void Finish() override
    {
        for (const auto& column : SourceTableSchema_.Columns()) {
            GetColumnPtr(column.Name, TableName_);
        }
    }

private:
    const TTableSchema SourceTableSchema_;
    TTableSchema* RefinedTableSchema_;
    TStringBuf TableName_;

    TScanSchemaProxy(
        TTableSchema* tableSchema,
        TTableSchema* refinedTableSchema,
        const TTableSchema& sourceTableSchema,
        const TStringBuf& tableName)
        : TSchemaProxy(tableSchema, tableName)
        , SourceTableSchema_(sourceTableSchema)
        , RefinedTableSchema_(refinedTableSchema)
        , TableName_(tableName)
    { }

    void Init(int keyColumnCount)
    {
        const auto& columns = SourceTableSchema_.Columns();
        int count = std::min(keyColumnCount, static_cast<int>(columns.size()));
        for (int i = 0; i < count; ++i) {
            GetColumnPtr(columns[i].Name, TableName_);
        }
    }

    DECLARE_NEW_FRIEND();
};

class TJoinSchemaProxy
    : public TSchemaProxy
{
public:
    TJoinSchemaProxy(
        TTableSchema* tableSchema,
        TSchemaProxyPtr self,
        TSchemaProxyPtr foreign)
        : TSchemaProxy(tableSchema)
        , Self_(self)
        , Foreign_(foreign)
    { }

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        const TColumnSchema* column = nullptr;

        if ((column = Self_->GetColumnPtr(name, tableName))) {
            if (Foreign_->GetColumnPtr(name, tableName)) {
                THROW_ERROR_EXCEPTION("Column %Qv occurs both in main and joined tables",
                    NAst::FormatColumn(name, tableName));
            }
        } else {
            column = Foreign_->GetColumnPtr(name, tableName);
        }

        return column;
    }

    virtual void Finish() override
    {
        Self_->Finish();
        Foreign_->Finish();

        for (const auto& column : Self_->GetLookup()) {
            GetColumnPtr(column.first.first, column.first.second);
        }

        for (const auto& column : Foreign_->GetLookup()) {
            GetColumnPtr(column.first.first, column.first.second);
        }
    }

private:
    TSchemaProxyPtr Self_;
    TSchemaProxyPtr Foreign_;

};

class TGroupSchemaProxy
    : public TSchemaProxy
{
public:
    TGroupSchemaProxy(
        TTableSchema* tableSchema,
        TSchemaProxyPtr base,
        TAggregateItemList* aggregateItems)
        : TSchemaProxy(tableSchema)
        , Base_(base)
        , AggregateItems_(aggregateItems)
    { }

    virtual const TColumnSchema* AddColumnPtr(const TStringBuf& name, const TStringBuf& tableName) override
    {
        return nullptr;
    }

    virtual TColumnSchema AddAggregateColumnPtr(
        const Stroka& name,
        const TAggregateTypeInferrer* aggregateFunction,
        const NAst::TExpression* argument,
        Stroka subexprName,
        Stroka source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap) override
    {
        auto typedOperand = Base_->BuildTypedExpression(
            argument,
            source,
            functions,
            aliasMap);

        auto stateType = aggregateFunction->InferStateType(typedOperand->Type, name, source);
        auto resultType = aggregateFunction->InferResultType(typedOperand->Type, name, source);

        CheckExpressionDepth(typedOperand);

        AggregateItems_->emplace_back(
            typedOperand,
            name,
            subexprName,
            stateType,
            resultType);

        return TColumnSchema(subexprName, resultType);
    }

private:
    TSchemaProxyPtr Base_;
    TAggregateItemList* AggregateItems_;

};

TConstExpressionPtr BuildWhereClause(
    const NAst::TExpressionList& expressionAst,
    const Stroka& source,
    const TSchemaProxyPtr& schemaProxy,
    const TTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionAst));
    }

    auto typedPredicate = schemaProxy->BuildTypedExpression(
        expressionAst.front().Get(),
        source,
        functions,
        aliasMap);

    CheckExpressionDepth(typedPredicate);

    auto actualType = typedPredicate->Type;
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("WHERE-clause is not a boolean expression")
            << TErrorAttribute("source", InferName(expressionAst))
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TConstGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    const TTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    auto groupClause = New<TGroupClause>();
    groupClause->IsMerge = false;
    groupClause->IsFinal = true;
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = schemaProxy->BuildTypedExpression(
            expressionAst.Get(),
            source,
            functions,
            aliasMap);

        CheckExpressionDepth(typedExpr);
        groupClause->AddGroupItem(typedExpr, InferName(expressionAst.Get()));
    }

    TTableSchema& tableSchema = groupClause->GroupedTableSchema;
    schemaProxy = New<TGroupSchemaProxy>(&tableSchema, std::move(schemaProxy), &groupClause->AggregateItems);

    return groupClause;
}

TConstExpressionPtr BuildHavingClause(
    const NAst::TExpressionList& expressionsAst,
    const Stroka& source,
    const TSchemaProxyPtr& schemaProxy,
    const TTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    if (expressionsAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", InferName(expressionsAst));
    }

    auto typedPredicate = schemaProxy->BuildTypedExpression(
        expressionsAst.front().Get(),
        source,
        functions,
        aliasMap);

    CheckExpressionDepth(typedPredicate);

    auto actualType = typedPredicate->Type;
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("HAVING-clause is not a boolean expression")
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return typedPredicate;
}

TConstProjectClausePtr BuildProjectClause(
    const NAst::TExpressionList& expressionsAst,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    const TTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    auto projectClause = New<TProjectClause>();

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = schemaProxy->BuildTypedExpression(
            expressionAst.Get(),
            source,
            functions,
            aliasMap);

        CheckExpressionDepth(typedExpr);
        projectClause->AddProjection(typedExpr, InferName(expressionAst.Get()));
    }

    schemaProxy = New<TSchemaProxy>(&projectClause->ProjectTableSchema);

    return projectClause;
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    const Stroka& source,
    TSchemaProxyPtr& schemaProxy,
    const TTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap)
{
    if (const auto* wherePredicate = ast.WherePredicate.GetPtr()) {
        query->WhereClause = BuildWhereClause(
            *wherePredicate,
            source,
            schemaProxy,
            functions,
            aliasMap);
    }

    if (const auto* groupExprs = ast.GroupExprs.GetPtr()) {
        query->GroupClause = BuildGroupClause(
            groupExprs->first,
            groupExprs->second,
            source,
            schemaProxy,
            functions,
            aliasMap);
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildHavingClause(
            ast.HavingPredicate.Get(),
            source,
            schemaProxy,
            functions,
            aliasMap);
    }

    if (!ast.OrderExpressions.empty()) {
        auto orderClause = New<TOrderClause>();

        for (const auto& orderExpr : ast.OrderExpressions) {
            for (const auto& expressionAst : orderExpr.first) {
                auto typedExpr = schemaProxy->BuildTypedExpression(
                    expressionAst.Get(),
                    source,
                    functions,
                    aliasMap);

                orderClause->OrderItems.emplace_back(typedExpr, orderExpr.second);
            }
        }

        query->OrderClause = std::move(orderClause);
    }

    if (ast.SelectExprs) {
        query->ProjectClause = BuildProjectClause(
            ast.SelectExprs.Get(),
            source,
            schemaProxy,
            functions,
            aliasMap);
    }

    schemaProxy->Finish();
}

void ParseYqlString(
    NAst::TAstHead* astHead,
    const Stroka& source,
    NAst::TParser::token::yytokentype strayToken)
{
    NAst::TLexer lexer(source, strayToken);
    NAst::TParser parser(lexer, astHead, source);

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            << TErrorAttribute("source", source);
    }
}

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(const std::vector<Stroka>& names, const TTypeInferrerMapPtr& typeInferrers)
{
    MergeFrom(typeInferrers.Get(), BuiltinTypeInferrersMap.Get());
}

////////////////////////////////////////////////////////////////////////////////

// For testing
void ParseJobQuery(const Stroka& source)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);
}

std::pair<TQueryPtr, TDataRanges> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    const TFetchFunctions& fetchFunctions,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseQuery);

    const auto& ast = astHead.first.As<NAst::TQuery>();

    auto functionNames = ExtractFunctionNames(std::make_pair(ast, astHead.second));

    auto functions = New<TTypeInferrerMap>();
    fetchFunctions(functionNames, functions);

    TDataSplit selfDataSplit;

    auto query = New<TQuery>(inputRowLimit, outputRowLimit, TGuid::Create());
    TSchemaProxyPtr schemaProxy;

    auto table = ast.Table;
    LOG_DEBUG("Getting initial data split for %v", table.Path);

    selfDataSplit = WaitFor(callbacks->GetInitialSplit(table.Path, timestamp))
        .ValueOrThrow();
    auto tableSchema = GetTableSchemaFromDataSplit(selfDataSplit);
    auto keySchema = tableSchema.ToKeys();

    query->KeyColumnsCount = tableSchema.GetKeyColumnCount();
    query->TableSchema = keySchema;

    schemaProxy = TScanSchemaProxy::Create(
        &query->RenamedTableSchema,
        &query->TableSchema,
        tableSchema,
        tableSchema.GetKeyColumnCount(),
        table.Alias);

    for (const auto& join : ast.Joins) {
        auto foreignDataSplit = WaitFor(callbacks->GetInitialSplit(join.Table.Path, timestamp))
            .ValueOrThrow();

        auto foreignTableSchema = GetTableSchemaFromDataSplit(foreignDataSplit);
        auto foreignKeyColumnsCount = foreignTableSchema.GetKeyColumns().size();

        auto joinClause = New<TJoinClause>();

        joinClause->ForeignKeyColumnsCount = foreignKeyColumnsCount;
        joinClause->ForeignDataId = GetObjectIdFromDataSplit(foreignDataSplit);
        joinClause->IsLeft = join.IsLeft;

        const auto& columns = foreignTableSchema.Columns();
        joinClause->ForeignTableSchema = TTableSchema(std::vector<TColumnSchema>(
            columns.begin(),
            columns.begin() + std::min(foreignKeyColumnsCount, columns.size())));

        auto foreignSourceProxy = TScanSchemaProxy::Create(
            &joinClause->RenamedTableSchema,
            &joinClause->ForeignTableSchema,
            foreignTableSchema,
            foreignKeyColumnsCount,
            join.Table.Alias);

        std::vector<std::pair<TConstExpressionPtr, TConstExpressionPtr>> equations;

        // Merge columns.
        for (const auto& reference : join.Fields) {
            const auto* selfColumn = schemaProxy->GetColumnPtr(
                reference->ColumnName,
                reference->TableName);
            const auto* foreignColumn = foreignSourceProxy->GetColumnPtr(
                reference->ColumnName,
                reference->TableName);

            if (!selfColumn || !foreignColumn) {
                THROW_ERROR_EXCEPTION("Column %Qv not found",
                    NAst::FormatColumn(reference->ColumnName, reference->TableName));
            }

            if (selfColumn->Type != foreignColumn->Type) {
                THROW_ERROR_EXCEPTION("Column type %Qv mismatch",
                    NAst::FormatColumn(reference->ColumnName, reference->TableName))
                    << TErrorAttribute("self_type", selfColumn->Type)
                    << TErrorAttribute("foreign_type", foreignColumn->Type);
            }

            equations.emplace_back(
                New<TReferenceExpression>(selfColumn->Type, selfColumn->Name),
                New<TReferenceExpression>(foreignColumn->Type, foreignColumn->Name));

            joinClause->JoinedTableSchema.AppendColumn(*selfColumn);
        }

        std::vector<TConstExpressionPtr> leftEquations;
        std::vector<TConstExpressionPtr> rightEquations;

        for (const auto& argument : join.Left) {
            leftEquations.push_back(schemaProxy->BuildTypedExpression(
                argument.Get(),
                source,
                functions,
                astHead.second));
        }

        for (const auto& argument : join.Right) {
            rightEquations.push_back(foreignSourceProxy->BuildTypedExpression(
                argument.Get(),
                source,
                functions,
                astHead.second));
        }

        if (leftEquations.size() != rightEquations.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                leftEquations.size(),
                rightEquations.size())
                << TErrorAttribute("lhs_source", InferName(join.Left))
                << TErrorAttribute("rhs_source", InferName(join.Right));
        }

        for (size_t index = 0; index < leftEquations.size(); ++index) {
            if (leftEquations[index]->Type != rightEquations[index]->Type) {
                THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                    InferName(leftEquations[index]),
                    InferName(rightEquations[index]))
                    << TErrorAttribute("self_type", leftEquations[index]->Type)
                    << TErrorAttribute("foreign_type", rightEquations[index]->Type);
            }

            equations.emplace_back(leftEquations[index], rightEquations[index]);
        }

        // If can use ranges, rearrange equations according to key columns and enrich with evaluated columns
        const auto& renamedTableSchema = joinClause->RenamedTableSchema;


        std::vector<int> equationByIndex(foreignKeyColumnsCount, -1);
        for (size_t equationIndex = 0; equationIndex < equations.size(); ++equationIndex) {
            const auto& column = equations[equationIndex];
            const auto& expr = column.second;

            if (const auto* refExpr = expr->As<TReferenceExpression>()) {
                auto index = renamedTableSchema.GetColumnIndexOrThrow(refExpr->ColumnName);
                if (index < foreignKeyColumnsCount) {
                    equationByIndex[index] = equationIndex;
                    continue;
                }
            }
            break;
        }

        std::vector<bool> usedJoinKeyEquations(equations.size(), false);

        std::vector<std::vector<int>> referenceIds(foreignKeyColumnsCount);
        std::vector<TConstExpressionPtr> evaluatedColumnExprs(foreignKeyColumnsCount);

        for (size_t index = 0; index < foreignKeyColumnsCount; ++index) {
            if (!foreignTableSchema.Columns()[index].Expression) {
                continue;
            }

            yhash_set<Stroka> references;
            evaluatedColumnExprs[index] = PrepareExpression(
                foreignTableSchema.Columns()[index].Expression.Get(),
                foreignTableSchema,
                functions,
                &references);

            for (const auto& reference : references) {
                referenceIds[index].push_back(foreignTableSchema.GetColumnIndexOrThrow(reference));
            }
            std::sort(referenceIds[index].begin(), referenceIds[index].end());
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < foreignKeyColumnsCount; ++keyPrefix) {
            if (equationByIndex[keyPrefix] >= 0) {
                usedJoinKeyEquations[equationByIndex[keyPrefix]] = true;
                continue;
            }

            if (foreignTableSchema.Columns()[keyPrefix].Expression) {
                const auto& references = referenceIds[keyPrefix];
                auto canEvaluate = true;
                for (int referenceIndex : references) {
                    if (equationByIndex[referenceIndex] < 0) {
                        canEvaluate = false;
                    }
                }
                if (canEvaluate) {
                    continue;
                }
            }
            break;
        }

        bool canUseSourceRanges = true;
        for (auto flag : usedJoinKeyEquations) {
            if (!flag) {
                canUseSourceRanges = false;
            }
        }

        joinClause->CanUseSourceRanges = canUseSourceRanges;
        if (canUseSourceRanges) {
            equationByIndex.resize(keyPrefix);
            joinClause->EvaluatedColumns.resize(keyPrefix);

            for (int column = 0; column < keyPrefix; ++column) {
                joinClause->EvaluatedColumns[column] = evaluatedColumnExprs[column];
            }
        } else {
            keyPrefix = equations.size();
            equationByIndex.resize(keyPrefix);
            joinClause->EvaluatedColumns.resize(keyPrefix);
            for (size_t index = 0; index < keyPrefix; ++index) {
                equationByIndex[index] = index;
            }
        }

        joinClause->EquationByIndex = equationByIndex;
        joinClause->KeyPrefix = keyPrefix;
        joinClause->Equations = std::move(equations);

        schemaProxy = New<TJoinSchemaProxy>(
            &joinClause->JoinedTableSchema,
            schemaProxy,
            foreignSourceProxy);

        query->JoinClauses.push_back(std::move(joinClause));
    }

    PrepareQuery(query, ast, source, schemaProxy, functions, astHead.second);

    if (ast.Limit) {
        query->Limit = ast.Limit;
    } else if (query->OrderClause) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    auto queryFingerprint = InferName(query, true);
    LOG_DEBUG("Prepared query (Fingerprint: %v, InputSchema: %v, RenamedSchema: %v, ResultSchema: %v)",
        queryFingerprint,
        NYTree::ConvertToYsonString(query->TableSchema, NYson::EYsonFormat::Text).Data(),
        NYTree::ConvertToYsonString(query->RenamedTableSchema, NYson::EYsonFormat::Text).Data(),
        NYTree::ConvertToYsonString(query->GetTableSchema(), NYson::EYsonFormat::Text).Data());

    auto range = GetBothBoundsFromDataSplit(selfDataSplit);

    SmallVector<TRowRange, 1> rowRanges;
    TRowBufferPtr buffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    rowRanges.push_back({
        buffer->Capture(range.first.Get()),
        buffer->Capture(range.second.Get())});

    TDataRanges dataSource;
    dataSource.Id = GetObjectIdFromDataSplit(selfDataSplit);
    dataSource.Ranges = MakeSharedRange(std::move(rowRanges), std::move(buffer));

    return std::make_pair(query, dataSource);
}

TQueryPtr PrepareJobQuery(
    const Stroka& source,
    const TTableSchema& tableSchema,
    const TFetchFunctions& fetchFunctions)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TQuery>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseJobQuery);

    auto& ast = astHead.first.As<NAst::TQuery>();

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto parsedQueryInfo = std::make_pair(ast, astHead.second);

    auto unlimited = std::numeric_limits<i64>::max();

    auto query = New<TQuery>(unlimited, unlimited, TGuid::Create());
    TSchemaProxyPtr schemaProxy = TScanSchemaProxy::Create(
        &query->RenamedTableSchema,
        &query->TableSchema,
        tableSchema);

    auto functionNames = ExtractFunctionNames(parsedQueryInfo);

    auto functions = New<TTypeInferrerMap>();
    fetchFunctions(functionNames, functions);

    PrepareQuery(
        query,
        parsedQueryInfo.first,
        source,
        schemaProxy,
        functions,
        parsedQueryInfo.second);

    return query;
}

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    yhash_set<Stroka>* references)
{
    NAst::TAstHead astHead{TVariantTypeTag<NAst::TExpressionPtr>(), NAst::TAliasMap()};
    ParseYqlString(
        &astHead,
        source,
        NAst::TParser::token::StrayWillParseExpression);

    auto& expr = astHead.first.As<NAst::TExpressionPtr>();

    auto schemaProxy = New<TSchemaProxy>(&tableSchema);

    return schemaProxy->BuildTypedExpression(
        expr.Get(),
        source,
        functions,
        astHead.second,
        references);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
