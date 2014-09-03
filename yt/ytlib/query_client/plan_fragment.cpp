#include "stdafx.h"

#include "plan_fragment.h"
#include "private.h"
#include "helpers.h"
#include "lexer.h"
#include "parser.hpp"
#include "callbacks.h"

#include <ytlib/new_table_client/schema.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/convert.h>

#include <ytlib/query_client/plan_fragment.pb.h>


namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NVersionedTableClient;

using NYT::ToProto;
using NYT::FromProto;

using NNodeTrackerClient::TNodeDirectory;

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

////////////////////////////////////////////////////////////////////////////////

Stroka TExpression::GetName() const
{
    return Stroka();
}

TKeyColumns TOperator::GetKeyColumns() const
{
    if (auto scanOp = this->As<TScanOperator>()) {
        return GetKeyColumnsFromDataSplit(scanOp->DataSplits[0]);
        // TODO(lukyan): check that other splits hava the same key columns
    } else if (auto filterOp = this->As<TFilterOperator>()) {
        return filterOp->Source->GetKeyColumns();
    } else if (this->As<TGroupOperator>() || this->As<TProjectOperator>()) {
        return TKeyColumns();
    } else {
        YUNREACHABLE();
    }
}

TTableSchema TScanOperator::GetTableSchema() const
{
    YCHECK(DataSplits.size() > 0);
    return GetTableSchemaFromDataSplit(DataSplits[0]);
}

TTableSchema TFilterOperator::GetTableSchema() const
{
    return Source->GetTableSchema();
}

TTableSchema TGroupOperator::GetTableSchema() const
{
    TTableSchema result;

    for (const auto& groupItem : GroupItems) {
        result.Columns().emplace_back(
            groupItem.Name,
            groupItem.Expression->Type);
    }

    for (const auto& aggregateItem : AggregateItems) {
        result.Columns().emplace_back(
            aggregateItem.Name,
            aggregateItem.Expression->Type);
    }

    ValidateTableScheme(result);

    return result;
}

TTableSchema TProjectOperator::GetTableSchema() const
{
    TTableSchema result;

    auto sourceSchema = Source->GetTableSchema();
    for (const auto& projection : Projections) {
        result.Columns().emplace_back(
            projection.Name,
            projection.Expression->Type);
    }

    ValidateTableScheme(result);

    return result;
}

EValueType InferBinaryExprType(EBinaryOp opCode, EValueType lhsType, EValueType rhsType, const TStringBuf& source)
{
    if (lhsType != rhsType) {
        THROW_ERROR_EXCEPTION(
            "Type mismatch in expression %Qv",
            source)
            << TErrorAttribute("lhs_type", ToString(lhsType))
            << TErrorAttribute("rhs_type", ToString(rhsType));
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
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EBinaryOp::Modulo:
            if (!IsIntegralType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires integral operands",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return operandType;

        case EBinaryOp::And:
        case EBinaryOp::Or:
            if (operandType != EValueType::Boolean) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires boolean operands",
                    source)
                    << TErrorAttribute("operand_type", ToString(operandType));
            }
            return EValueType::Boolean;

        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::Greater:
            return EValueType::Boolean;

        case EBinaryOp::LessOrEqual:
        case EBinaryOp::GreaterOrEqual:
            if (!IsArithmeticType(operandType)) {
                THROW_ERROR_EXCEPTION(
                    "Expression %Qv requires either integral or floating-point operands",
                    source)
                    << TErrorAttribute("lhs_type", ToString(operandType));
            }
            return EValueType::Boolean;

        default:
            YUNREACHABLE();
    }
}

EValueType InferFunctionExprType(Stroka functionName, const std::vector<EValueType>& argTypes, const TStringBuf& source)
{
    functionName.to_lower();

    auto validateArgCount = [&] (int argCount) {
        if (argTypes.size() != argCount) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv expects %v arguments, but %v provided",
                functionName,
                argCount,
                argTypes.size())
                << TErrorAttribute("expression", source);
        }
    };

    auto checkTypeCast = [&] (EValueType destType) {
        validateArgCount(1);
        auto argType = argTypes[0];

        if (argType != EValueType::Int64 && argType != EValueType::Uint64 && argType != EValueType::Double) {
            THROW_ERROR_EXCEPTION("Conversion %Qv is not supported for this types", source)
                << TErrorAttribute("src_type", ToString(argType))
                << TErrorAttribute("dest_type", ToString(destType));
        }

        return destType;
    };

    if (functionName == "if") {
        validateArgCount(3);

        auto conditionType = argTypes[0];
        auto thenType = argTypes[1];
        auto elseType = argTypes[2];

        if (conditionType != EValueType::Boolean) {
            THROW_ERROR_EXCEPTION("Expected condition %Qv to be boolean", source)
                << TErrorAttribute("condition_type", ToString(conditionType));
        }

        if (thenType != elseType) {
            THROW_ERROR_EXCEPTION(
                "Type mismatch in expression %Qv",
                source)
                << TErrorAttribute("then_type", ToString(thenType))
                << TErrorAttribute("else_type", ToString(elseType));
        }

        return thenType;
    } else if (functionName == "is_prefix") {
        validateArgCount(2);

        auto lhsType = argTypes[0];
        auto rhsType = argTypes[1];

        if (lhsType != EValueType::String || rhsType != EValueType::String) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv supports only string arguments",
                source)
                << TErrorAttribute("lhs_type", ToString(lhsType))
                << TErrorAttribute("rhs_type", ToString(rhsType));
        }

        return EValueType::Boolean;
    } else if (functionName == "lower") {
        validateArgCount(1);
        auto argType = argTypes[0];

        if (argType != EValueType::String) {
            THROW_ERROR_EXCEPTION(
                "Expression %Qv supports only string argument",
                source)
                << TErrorAttribute("arg_type", ToString(argType));
        }

        return EValueType::String;
    } else if (functionName == "is_null") {
        validateArgCount(1);
        return EValueType::Boolean;
    } else if (functionName == "int64") {
        return checkTypeCast(EValueType::Int64);
    } else if (functionName == "uint64") {
        return checkTypeCast(EValueType::Uint64);
    } else if (functionName == "double") {
        return checkTypeCast(EValueType::Double);
    }

    THROW_ERROR_EXCEPTION(
        "Unknown function in expression %Qv",
        source)
        << TErrorAttribute("function_name", functionName);

}

void CheckDepth(const TOperatorPtr& head)
{
    std::function<int(const TConstExpressionPtr& op)> getExpressionDepth = [&] (const TConstExpressionPtr& op) -> int {
        if (op->As<TLiteralExpression>() || op->As<TReferenceExpression>()) {
            return 1;
        } else if (auto functionExpr = op->As<TFunctionExpression>()) {
            int maxChildDepth = 0;
            for (const auto& argument : functionExpr->Arguments) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(argument));
            }
            return maxChildDepth + 1;
        } else if (auto binaryOpExpr = op->As<TBinaryOpExpression>()) {
            return std::max(
                getExpressionDepth(binaryOpExpr->Lhs), 
                getExpressionDepth(binaryOpExpr->Rhs)) + 1;
        }
        YUNREACHABLE();
    };

    std::function<int(const TConstOperatorPtr& op)> getOperatorDepth = [&] (const TConstOperatorPtr& op) -> int {
        if (op->As<TScanOperator>()) {
            return 1;
        } else if (auto filterOp = op->As<TFilterOperator>()) {
            return std::max(
                getOperatorDepth(filterOp->Source), 
                getExpressionDepth(filterOp->Predicate)) + 1;
        } else if (auto projectOp = op->As<TProjectOperator>()) {
            int maxChildDepth = getOperatorDepth(projectOp->Source);
            for (const auto& projection : projectOp->Projections) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(projection.Expression) + 1);
            }
            return maxChildDepth + 1;
        } else if (auto groupOp = op->As<TGroupOperator>()) {
            int maxChildDepth = getOperatorDepth(groupOp->Source);
            for (const auto& groupItem : groupOp->GroupItems) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(groupItem.Expression) + 1);
            }
            for (const auto& aggregateItem : groupOp->AggregateItems) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(aggregateItem.Expression) + 1);
            }
            return maxChildDepth + 1;
        }
        YUNREACHABLE();
    };

    if (getOperatorDepth(head) > PlanFragmentDepthLimit) {
        THROW_ERROR_EXCEPTION("Plan fragment depth limit exceeded");
    }
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
{
    TRowBuffer rowBuffer;

    NAst::TLexer lexer(&rowBuffer, source,  NAst::TParser::token::StrayWillParseQuery);

    NAst::TQuery query;
    NAst::TParser parser(lexer, &query);

    int result = parser.parse();
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to parse query");
    }

    ////////////////////////////////////
    auto getAggregate = [] (TStringBuf functionName) {
        Stroka name(functionName);
        name.to_lower();

        TNullable<EAggregateFunctions> result;

        if (name == "sum") {
            result.Assign(EAggregateFunctions::Sum);
        } else if (name == "min") {
            result.Assign(EAggregateFunctions::Min);
        } else if (name == "max") {
            result.Assign(EAggregateFunctions::Max);
        } else if (name == "avg") {
            result.Assign(EAggregateFunctions::Average);
        } else if (name == "count") {
            result.Assign(EAggregateFunctions::Count);
        }

        return result;
    };

    auto planFragment = New<TPlanFragment>(timestamp, inputRowLimit, outputRowLimit, TGuid::Create(), source);
    planFragment->NodeDirectory = New<TNodeDirectory>();

    struct TTableSchemaProxy
    {
        TTableSchema TableSchema;
        std::set<Stroka>* LiveColumns;
        
        explicit TTableSchemaProxy(
            const TTableSchema& tableSchema,
            std::set<Stroka>* liveColumns = nullptr)
            : TableSchema(tableSchema)
            , LiveColumns(liveColumns)
        { }

        const TColumnSchema& operator [] (size_t index) const
        {
            return TableSchema.Columns()[index];
        }

        size_t GetColumnIndex(const TStringBuf& name) const
        {
            if (LiveColumns) {
                LiveColumns->emplace(name);
            }

            auto* column = TableSchema.FindColumn(name);
            if (!column) {
                THROW_ERROR_EXCEPTION("Undefined reference %Qv", name);
            }
            
            return TableSchema.GetColumnIndex(*column);
        }
    };

    struct TGroupOperatorProxy
    {
        TTableSchemaProxy SourceSchemaProxy;
        TGroupOperator& Op;
        std::map<Stroka, size_t> SubexprNames;        

        TGroupOperatorProxy(
            const TTableSchemaProxy& sourceSchemaProxy, 
            TGroupOperator& op)
            : SourceSchemaProxy(sourceSchemaProxy)
            , Op(op)
        { }

    };

    auto tablePath = query.FromPath;

    LOG_DEBUG("Getting initial data split for %v", tablePath);
    // XXX(sandello): We have just one table at the moment.
    // Will put TParallelAwaiter here in case of multiple tables.

    auto dataSplitOrError = WaitFor(callbacks->GetInitialSplit(
        tablePath,
        timestamp));
    THROW_ERROR_EXCEPTION_IF_FAILED(
        dataSplitOrError,
        "Failed to get initial data split for table %v",
        tablePath);

    auto querySourceString = planFragment->GetSource();

    TOwningRowBuilder rowBuilder;

    std::function<TExpressionPtr(
        const TTableSchemaProxy& tableSchema,
        const NAst::TExpression* expr,
        TGroupOperatorProxy* groupProxy)>
        buildTypedExpression = [&] (
            const TTableSchemaProxy& tableSchema,
            const NAst::TExpression* expr,
            TGroupOperatorProxy* groupProxy) -> TExpressionPtr {

        if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
            return New<TLiteralExpression>(
                literalExpr->SourceLocation,
                EValueType(literalExpr->Value.Type),
                rowBuilder.AddValue(literalExpr->Value));
        } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
            size_t index = tableSchema.GetColumnIndex(referenceExpr->ColumnName);

            return New<TReferenceExpression>(
                referenceExpr->SourceLocation,
                tableSchema[index].Type,
                referenceExpr->ColumnName);
        } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
            auto functionName = functionExpr->FunctionName;

            auto aggregateFunction = getAggregate(functionName);

            if (aggregateFunction) {
                if (!groupProxy) {
                    THROW_ERROR_EXCEPTION(
                        "Misuse of aggregate function %v",
                        aggregateFunction.Get())
                        << TErrorAttribute("source", functionExpr->GetSource(querySourceString));
                }
                
                auto& groupOp = groupProxy->Op;

                if (functionExpr->Arguments.size() != 1) {
                    THROW_ERROR_EXCEPTION(
                        "Aggregate function %Qv must have exactly one argument",
                        aggregateFunction.Get())
                        << TErrorAttribute("source", functionExpr->GetSource(querySourceString));
                }

                auto subexprName = InferName(functionExpr);
                auto emplaced = groupProxy->SubexprNames.emplace(subexprName, groupOp.AggregateItems.size());
                if (emplaced.second) {
                    groupOp.AggregateItems.emplace_back(
                        buildTypedExpression(
                            groupProxy->SourceSchemaProxy,
                            functionExpr->Arguments.front().Get(),
                            nullptr),
                        aggregateFunction.Get(),
                        subexprName);
                }

                return New<TReferenceExpression>(
                    NullSourceLocation,
                    groupOp.AggregateItems[emplaced.first->second].Expression->Type,
                    subexprName);
            } else {
                TFunctionExpression::TArguments arguments;
                std::vector<EValueType> types;

                for (const auto& argument : functionExpr->Arguments) {
                    auto typedArgument = buildTypedExpression(tableSchema, argument.Get(), groupProxy);

                    arguments.push_back(typedArgument);
                    types.push_back(typedArgument->Type);
                }

                return New<TFunctionExpression>(
                    functionExpr->SourceLocation,
                    InferFunctionExprType(functionName, types, functionExpr->GetSource(querySourceString)),
                    functionName,
                    arguments);
            }
        } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
            auto typedLhsExpr = buildTypedExpression(tableSchema, binaryExpr->Lhs.Get(), groupProxy);
            auto typedRhsExpr = buildTypedExpression(tableSchema, binaryExpr->Rhs.Get(), groupProxy);

            return New<TBinaryOpExpression>(
                binaryExpr->SourceLocation,
                InferBinaryExprType(
                    binaryExpr->Opcode,
                    typedLhsExpr->Type,
                    typedRhsExpr->Type,
                    binaryExpr->GetSource(querySourceString)),
                binaryExpr->Opcode,
                typedLhsExpr,
                typedRhsExpr);
        } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
            auto sourceLoaction = inExpr->SourceLocation;
            auto inExprOperand = buildTypedExpression(tableSchema, inExpr->Expr.Get(), groupProxy);

            std::function<TExpressionPtr(const TValue*, const TValue*)> makeOrExpression = 
                [&] (const TValue* begin, const TValue* end) -> TExpressionPtr {
                if (begin == end) {
                    return New<TLiteralExpression>(
                        sourceLoaction,
                        EValueType::Boolean,
                        rowBuilder.AddValue(MakeUnversionedBooleanValue(false)));
                } else if (begin + 1 == end) {

                    auto literalExpr = New<TLiteralExpression>(
                        sourceLoaction,
                        EValueType(begin->Type),
                        rowBuilder.AddValue(*begin));

                    return New<TBinaryOpExpression>(
                        sourceLoaction,
                        EValueType::Boolean,
                        EBinaryOp::Equal,
                        inExprOperand,
                        literalExpr);
                } else {
                    auto middle = (end - begin) / 2;

                    return New<TBinaryOpExpression>(
                        sourceLoaction,
                        EValueType::Boolean,
                        EBinaryOp::Or, 
                        makeOrExpression(begin, begin + middle),
                        makeOrExpression(begin + middle, end));
                }
            };

            return makeOrExpression(inExpr->Values.data(), inExpr->Values.data() + inExpr->Values.size());
        }

        YUNREACHABLE();
    };
    
    ////////////////////////////////////
    
    std::set<Stroka> liveColumns;
    auto scanOp = New<TScanOperator>();
    auto initialDataSplit = dataSplitOrError.Value();
    auto initialTableSchema = GetTableSchemaFromDataSplit(initialDataSplit);

    TOperatorPtr head = scanOp;
    TTableSchemaProxy tableSchemaProxy(initialTableSchema, &liveColumns);
    TNullable<TGroupOperatorProxy> groupOpProxy;
    
    if (query.WherePredicate) {
        auto filterOp = New<TFilterOperator>();
        filterOp->Source = head;
        filterOp->Predicate = buildTypedExpression(tableSchemaProxy, query.WherePredicate.Get(), nullptr);

        auto actualType = filterOp->Predicate->Type;
        EValueType expectedType(EValueType::Boolean);
        if (actualType != expectedType) {
            THROW_ERROR_EXCEPTION("WHERE-clause is not a boolean expression")
                << TErrorAttribute("actual_type", actualType)
                << TErrorAttribute("expected_type", expectedType);
        }

        head = filterOp;
    }   

    if (query.GroupExprs) {
        auto groupOp = New<TGroupOperator>();
        groupOp->Source = head;

        TTableSchema tableSchema;
        for (const auto& expr : query.GroupExprs.Get()) {
            auto typedExpr = buildTypedExpression(tableSchemaProxy, expr.first.Get(), nullptr);
            groupOp->GroupItems.emplace_back(typedExpr, expr.second);
            tableSchema.Columns().emplace_back(expr.second, typedExpr->Type);
        }

        ValidateTableScheme(tableSchema);

        head = groupOp;
        groupOpProxy.Emplace(tableSchemaProxy, *groupOp);
        tableSchemaProxy = TTableSchemaProxy(tableSchema);
    }

    if (query.SelectExprs) {
        auto projectOp = New<TProjectOperator>();
        projectOp->Source = head;

        TTableSchema tableSchema;
        for (const auto& expr : query.SelectExprs.Get()) {
            auto typedExpr = buildTypedExpression(tableSchemaProxy, expr.first.Get(), groupOpProxy.GetPtr());
            projectOp->Projections.emplace_back(typedExpr, expr.second);
            tableSchema.Columns().emplace_back(expr.second, typedExpr->Type);
        }

        ValidateTableScheme(tableSchema);

        head = projectOp;
        groupOpProxy.Reset();
        tableSchemaProxy = TTableSchemaProxy(tableSchema);
    }

    planFragment->Literals = rowBuilder.GetRowAndReset();

    // Now we have planOperator and tableSchemaProxy
    
    // Prune references

    auto& columns = initialTableSchema.Columns();

    if (!tableSchemaProxy.LiveColumns) {
        columns.erase(
            std::remove_if(
                columns.begin(),
                columns.end(),
                [&liveColumns] (const TColumnSchema& columnSchema) {
                    return liveColumns.find(columnSchema.Name) == liveColumns.end();
                }),
            columns.end());
    }

    SetTableSchema(&initialDataSplit, initialTableSchema);
    scanOp->DataSplits.push_back(initialDataSplit);

    CheckDepth(head);

    planFragment->Head = head;

    return planFragment;
}

TPlanFragmentPtr TPlanFragment::RewriteWith(const TConstOperatorPtr& head) const
{
    auto fragment = New<TPlanFragment>(
        GetTimestamp(),
        GetInputRowLimit(),
        GetOutputRowLimit(),
        TGuid::Create(),
        GetSource());

    fragment->NodeDirectory = NodeDirectory;
    fragment->Literals = Literals;
    fragment->Head = head;
    
    return fragment;
}
////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TConstExpressionPtr& original)
{
    serialized->set_type(original->Type);
    serialized->set_location_begin(original->SourceLocation.first);
    serialized->set_location_end(original->SourceLocation.second);

    if (auto literalExpr = original->As<TLiteralExpression>()) {
        serialized->set_kind(EExpressionKind::Literal);
        auto* proto = serialized->MutableExtension(NProto::TLiteralExpression::literal_expression);
        proto->set_index(literalExpr->Index);
    } else if (auto referenceExpr = original->As<TReferenceExpression>()) {
        serialized->set_kind(EExpressionKind::Reference);
        auto* proto = serialized->MutableExtension(NProto::TReferenceExpression::reference_expression);
        proto->set_column_name(referenceExpr->ColumnName);
    } else if (auto functionExpr = original->As<TFunctionExpression>()) {
        serialized->set_kind(EExpressionKind::Function);
        auto* proto = serialized->MutableExtension(NProto::TFunctionExpression::function_expression);
        proto->set_function_name(functionExpr->FunctionName);
        ToProto(proto->mutable_arguments(), functionExpr->Arguments);
    } else if (auto binaryOpExpr = original->As<TBinaryOpExpression>()) {
        serialized->set_kind(EExpressionKind::BinaryOp);
        auto* proto = serialized->MutableExtension(NProto::TBinaryOpExpression::binary_op_expression);
        proto->set_opcode(binaryOpExpr->Opcode);
        ToProto(proto->mutable_lhs(), binaryOpExpr->Lhs);
        ToProto(proto->mutable_rhs(), binaryOpExpr->Rhs);
    } else {
        YUNREACHABLE();
    }
}

TExpressionPtr FromProto(const NProto::TExpression& serialized)
{
    auto kind = EExpressionKind(serialized.kind());
    auto type = EValueType(serialized.type());
    TSourceLocation sourceLocation(serialized.location_begin(), serialized.location_end());

    switch (kind) {
        case EExpressionKind::Literal: {
            auto typedResult = New<TLiteralExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TLiteralExpression::literal_expression);
            typedResult->Index = data.index();
            return typedResult;
        }

        case EExpressionKind::Reference: {
            auto typedResult = New<TReferenceExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            typedResult->ColumnName = data.column_name();
            return typedResult;
        }

        case EExpressionKind::Function: {
            auto typedResult = New<TFunctionExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            typedResult->FunctionName = data.function_name();
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }
            return typedResult;
        }

        case EExpressionKind::BinaryOp: {
            auto typedResult = New<TBinaryOpExpression>(sourceLocation, type);
            auto data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            typedResult->Opcode = EBinaryOp(data.opcode());
            typedResult->Lhs = FromProto(data.lhs());
            typedResult->Rhs = FromProto(data.rhs());
            return typedResult;
        } 
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNamedItem* serialized, const TNamedItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TAggregateItem* serialized, const TAggregateItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_aggregate_function(original.AggregateFunction);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TOperator* serialized, const TConstOperatorPtr& original)
{
    if (auto scanOp = original->As<TScanOperator>()) {
        serialized->set_kind(EOperatorKind::Scan);
        auto* proto = serialized->MutableExtension(NProto::TScanOperator::scan_operator);
        ToProto(proto->mutable_data_split(), scanOp->DataSplits);
    } else if (auto filterOp = original->As<TFilterOperator>()) {
        serialized->set_kind(EOperatorKind::Filter);
        auto* proto = serialized->MutableExtension(NProto::TFilterOperator::filter_operator);
        ToProto(proto->mutable_source(), filterOp->Source);
        ToProto(proto->mutable_predicate(), filterOp->Predicate);
    } else if (auto groupOp = original->As<TGroupOperator>()) {
        serialized->set_kind(EOperatorKind::Group);
        auto* proto = serialized->MutableExtension(NProto::TGroupOperator::group_operator);
        ToProto(proto->mutable_source(), groupOp->Source);
        ToProto(proto->mutable_group_items(), groupOp->GroupItems);
        ToProto(proto->mutable_aggregate_items(), groupOp->AggregateItems);
    } else if (auto projectOp = original->As<TProjectOperator>()) {
        serialized->set_kind(EOperatorKind::Project);
        auto* proto = serialized->MutableExtension(NProto::TProjectOperator::project_operator);
        ToProto(proto->mutable_source(), projectOp->Source);
        ToProto(proto->mutable_projections(), projectOp->Projections);
    } else {
        YUNREACHABLE();
    }    
}

TNamedItem FromProto(const NProto::TNamedItem& serialized)
{
    return TNamedItem(
        FromProto(serialized.expression()),
        serialized.name());
}

TAggregateItem FromProto(const NProto::TAggregateItem& serialized)
{
    return TAggregateItem(
        FromProto(serialized.expression()),
        EAggregateFunctions(serialized.aggregate_function()),
        serialized.name());
}

TOperatorPtr FromProto(const NProto::TOperator& serialized)
{
    auto kind = EOperatorKind(serialized.kind());

    switch (kind) {
        case EOperatorKind::Scan: {
            auto typedResult = New<TScanOperator>();
            auto data = serialized.GetExtension(NProto::TScanOperator::scan_operator);
            typedResult->DataSplits.reserve(data.data_split_size());
            for (int i = 0; i < data.data_split_size(); ++i) {
                TDataSplit dataSplit;
                FromProto(&dataSplit, data.data_split(i));
                typedResult->DataSplits.push_back(dataSplit);
            }
            return typedResult;
        }

        case EOperatorKind::Filter: {
            auto typedResult = New<TFilterOperator>();
            auto data = serialized.GetExtension(NProto::TFilterOperator::filter_operator);            
            typedResult->Source = FromProto(data.source());
            typedResult->Predicate = FromProto(data.predicate());
            return typedResult;
        }

        case EOperatorKind::Group: {
            auto typedResult = New<TGroupOperator>();
            auto data = serialized.GetExtension(NProto::TGroupOperator::group_operator);
            typedResult->Source = FromProto(data.source());
            typedResult->GroupItems.reserve(data.group_items_size());
            for (int i = 0; i < data.group_items_size(); ++i) {
                typedResult->GroupItems.push_back(FromProto(data.group_items(i)));
            }
            typedResult->AggregateItems.reserve(data.aggregate_items_size());
            for (int i = 0; i < data.aggregate_items_size(); ++i) {
                typedResult->AggregateItems.push_back( FromProto(data.aggregate_items(i)));
            }
            return typedResult;
        }

        case EOperatorKind::Project: {
            auto typedResult = New<TProjectOperator>();
            auto data = serialized.GetExtension(NProto::TProjectOperator::project_operator);
            typedResult->Source = FromProto(data.source());
            typedResult->Projections.reserve(data.projections_size());
            for (int i = 0; i < data.projections_size(); ++i) {
                typedResult->Projections.push_back(FromProto(data.projections(i)));
            }
            return typedResult;
        }
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TConstPlanFragmentPtr& fragment)
{
    ToProto(serialized->mutable_id(), fragment->GetId());
    ToProto(serialized->mutable_head(), fragment->Head);
    ToProto(serialized->mutable_literals(), fragment->Literals);
    
    serialized->set_timestamp(fragment->GetTimestamp());
    serialized->set_input_row_limit(fragment->GetInputRowLimit());
    serialized->set_output_row_limit(fragment->GetOutputRowLimit());
    serialized->set_source(fragment->GetSource());
}

TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized)
{
    auto result = New<TPlanFragment>(
        serialized.timestamp(),
        serialized.input_row_limit(),
        serialized.output_row_limit(),
        NYT::FromProto<TGuid>(serialized.id()),
        serialized.source());

    result->NodeDirectory = New<TNodeDirectory>();

    result->Head = FromProto(serialized.head());
    FromProto(&result->Literals, serialized.literals());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
