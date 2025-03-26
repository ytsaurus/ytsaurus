#include "computed_columns.h"

#include "conversion.h"
#include "helpers.h"
#include "format.h"
#include "config.h"

#include <yt/yt/library/codegen_api/execution_backend.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/client/table_client/logical_type.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmacro-redefined"
#endif

#include <yt/yt/library/query/engine/folding_profiler.h>

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/makeASTForLogicalFunction.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ActionsVisitor.h>

#include <DataTypes/DataTypeTuple.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

struct TComputedColumnEntry
{
    std::vector<std::string> References;
    TConstExpressionPtr Expression;
    std::string ComputedColumn;
    TLogicalTypePtr LogicalType;
};

//! Representation or an inclusion statement.
//! Example:
//!   #ColumnNames = {"key", "computed_key"};
//!   #PossibleTuples = {{2, 1234}, {5, 6789}, {-1, 4242}}.
//! In this case result corresponds to the statement:
//!   (key, computed_key) in ((2, 1234), (5, 6789), (-1, 4242)).
struct TInclusionStatement
{
    std::vector<std::string> ColumnNames;
    THashMap<std::string, int> ColumnPosition;
    std::vector<DB::FieldVector> PossibleTuples;

    TInclusionStatement(std::vector<std::string> columnNames, std::vector<DB::FieldVector> possibleTuples)
        : ColumnNames(std::move(columnNames))
        , PossibleTuples(std::move(possibleTuples))
    {
        // NB: if some column name appears twice in the list, we do not mind. The resulting deduction AST will still be correct.
        for (const auto& [index, columnName] : Enumerate(ColumnNames)) {
            ColumnPosition[columnName] = index;
        }
        for (const auto& tuple : PossibleTuples) {
            YT_VERIFY(tuple.size() == ColumnNames.size());
        }
    }

    bool ContainsReferences(const std::vector<std::string>& references) const
    {
        for (const auto& item : references) {
            if (!ColumnPosition.contains(item)) {
                return false;
            }
        }
        return true;
    }

    TInclusionStatement Filter(const std::vector<std::string>& references) const
    {
        std::vector<DB::FieldVector> filteredTuples(PossibleTuples.size());
        for (const auto& reference : references) {
            auto columnIndex = GetOrCrash(ColumnPosition, reference);
            for (size_t tupleIndex = 0; tupleIndex < PossibleTuples.size(); ++tupleIndex) {
                filteredTuples[tupleIndex].emplace_back(PossibleTuples[tupleIndex][columnIndex]);
            }
        }
        return TInclusionStatement(references, filteredTuples);
    }
};

std::vector<DB::FieldVector> Transpose(DB::FieldVector fields)
{
    std::vector<DB::FieldVector> result;
    result.reserve(fields.size());
    for (const auto& field : fields) {
        result.emplace_back(DB::FieldVector{field});
    }
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TInclusionStatement& statement, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Columns: %v, PossibleTuples: %v}",
        statement.ColumnNames,
        statement.PossibleTuples);
}

std::optional<std::vector<std::string>> IdentifierTupleToColumnNames(const DB::IAST& ast)
{
    if (auto* functionAst = ast.as<DB::ASTFunction>()) {
        if (functionAst->name != "tuple") {
            return std::nullopt;
        }
        std::vector<std::string> result;
        for (const auto& childAst : functionAst->arguments->children) {
            if (const auto* identifierAst = childAst->as<DB::ASTIdentifier>()) {
                result.emplace_back(identifierAst->shortName());
            } else {
                return std::nullopt;
            }
        }
        return result;
    }
    return std::nullopt;
}

class TComputedColumnPopulationQueryTreeVisitor : public DB::InDepthQueryTreeVisitor<TComputedColumnPopulationQueryTreeVisitor>
{
public:
    struct Data : public DB::WithContext
    {
        const std::vector<TComputedColumnEntry>& Entries;
        const TTableSchemaPtr& TableSchema;
        DB::PreparedSets& PreparedSets;
        const TQuerySettingsPtr& Settings;
        TLogger Logger;
    };

    TComputedColumnPopulationQueryTreeVisitor(Data& data)
        : Data_(data)
    { }

    bool shouldTraverseTopToBottom() const
    {
        return false;
    }

    bool needChildVisit(const DB::QueryTreeNodePtr& node, const DB::QueryTreeNodePtr& /*child*/) const
    {
        return node->getNodeType() != DB::QueryTreeNodeType::QUERY;
    }

    void visitImpl(DB::QueryTreeNodePtr& node)
    {
        // key == 5 -> ... AND ((key, computed_key) in ((5, f(5)))).
        // key in (4, 5) -> ... AND ((key, computed_key) in ((4, f(4)), (5, f(5)))).
        // (key, smth) == (4, 'x') -> ... AND ((key, computed_key) in ((4, f(4)))).
        if (node->getNodeType() == DB::QueryTreeNodeType::FUNCTION) {
            auto functionAst = node->toAST({.add_cast_for_constants = false})->as<DB::ASTFunction&>();
            if (auto rewrittenNode = TryRewrite(functionAst)) {
                node = rewrittenNode;
                return;
            }
        }
    }

private:
    TInclusionStatement DeduceComputedColumn(
        const TComputedColumnEntry& entry,
        const TInclusionStatement& statement)
    {
        const auto& Logger = Data_.Logger;

        YT_LOG_TRACE(
            "Deducing computed column value (ComputedColumn: %v, OriginalStatement: %v, Type: %v)",
            entry.ComputedColumn,
            statement,
            *entry.Expression->LogicalType);

        auto rowBuffer = New<TRowBuffer>();

        std::vector<DB::FieldVector> ResultTuples;

        for (auto possibleTuple : statement.PossibleTuples) {
            // Convert field to YT unversioned value.
            std::vector<TUnversionedValue> referenceValues(possibleTuple.size());
            for (const auto& [fieldIndex, field] : Enumerate(possibleTuple)) {
                auto& referenceValue = referenceValues[fieldIndex];
                referenceValue.Id = fieldIndex;
                ToUnversionedValue(field, &referenceValue);
                YT_LOG_TRACE(
                    "Converted reference field to YT unversioned value (Value: %v, Field: %v)",
                    referenceValue,
                    field);
            }

            TUnversionedValue resultValue{};
            try {
                TCGVariables variables;

                auto image = Profile(
                    entry.Expression,
                    Data_.TableSchema->Filter(entry.References),
                    nullptr,
                    &variables,
                    /*useCanonicalNullRelations*/ false,
                    /*executionBackend*/ NCodegen::EExecutionBackend::Native)();

                auto instance = image.Instantiate();

                auto finalizer = Finally([&] () {
                    variables.Clear();
                });

                instance.Run(
                    variables.GetLiteralValues(),
                    variables.GetOpaqueData(),
                    variables.GetOpaqueDataSizes(),
                    &resultValue,
                    TRange<TUnversionedValue>(referenceValues.data(), referenceValues.size()),
                    rowBuffer);

                resultValue.Id = 0;
            } catch (const TErrorException& ex) {
                YT_LOG_TRACE(ex, "Caught exception while evaluating expression");
                // Skip this value. We just deduced that it actually can't take supposed value
                // (YT would not allow writing value which results in exception in computed
                // column computation).
                continue;
            }
            YT_LOG_TRACE("Calculated expression result (ComputedColumnValue: %v)", resultValue);

            auto resultField = ToField(resultValue, entry.LogicalType);
            YT_LOG_TRACE("Converted to CH field (ComputedColumnValue: %v)", resultField);
            possibleTuple.emplace_back(std::move(resultField));
            ResultTuples.emplace_back(std::move(possibleTuple));
        }

        auto columnNames = statement.ColumnNames;
        columnNames.push_back(entry.ComputedColumn);

        TInclusionStatement resultStatement(std::move(columnNames), std::move(ResultTuples));

        YT_LOG_TRACE(
            "Deduced computed values (ResultStatement: %v)",
            resultStatement);

        return resultStatement;
    }

    void PopulatePreparedSets(DB::ASTPtr literal, std::vector<DB::DataTypePtr> dataTypes)
    {
        const auto& Logger = Data_.Logger;

        YT_LOG_TRACE("Populating prepared set for literal (Literal: %v)", literal);

        // Part below is done similarly to DB::makeExplicitSet.
        const auto& context = Data_.getContext();
        auto setElementKeys = DB::Set::getElementTypes(dataTypes, context->getSettingsRef().transform_null_in);
        auto setKey = literal->getTreeHash(/*ignore_aliases*/ true);
        if (Data_.PreparedSets.findTuple(setKey, setElementKeys)) {
            // Already prepared.
            return;
        }

        DB::Block block;
        const auto& functionAst = std::dynamic_pointer_cast<DB::ASTFunction>(literal);
        if (functionAst && (functionAst->name == "tuple" || functionAst->name == "array")) {
            block = DB::createBlockForSet(std::make_shared<DB::DataTypeTuple>(dataTypes), functionAst, dataTypes, context);
        } else {
            YT_ABORT();
        }

        Data_.PreparedSets.addFromTuple(setKey, block, context->getSettingsRef());
    }

    DB::ASTPtr PrepareInStatement(const TInclusionStatement& resultStatement)
    {
        DB::ASTs innerTupleAsts;
        for (const auto& tuple : resultStatement.PossibleTuples) {
            auto innerTupleAst = DB::makeASTFunction("tuple");
            for (const auto& field : tuple) {
                innerTupleAst->arguments->children.emplace_back(std::make_shared<DB::ASTLiteral>(field));
            }
            innerTupleAsts.emplace_back(std::move(innerTupleAst));
        }

        auto outerTupleAst = DB::makeASTFunction("tuple");
        outerTupleAst->arguments->children = innerTupleAsts;

        DB::ASTs columnAsts;

        std::vector<DB::DataTypePtr> dataTypes;
        for (const auto& columnName : resultStatement.ColumnNames) {
            auto* columnSchema = Data_.TableSchema->FindColumn(columnName);
            YT_VERIFY(columnSchema);
            TComplexTypeFieldDescriptor descriptor(*columnSchema);
            dataTypes.emplace_back(ToDataType(descriptor, Data_.Settings->Composite));
            columnAsts.emplace_back(std::make_shared<DB::ASTIdentifier>(columnName));
        }

        auto columnTupleAst = DB::makeASTFunction("tuple");
        columnTupleAst->arguments->children = std::move(columnAsts);

        auto inAst = DB::makeASTFunction("in", columnTupleAst, outerTupleAst);

        PopulatePreparedSets(outerTupleAst, dataTypes);

        return inAst;
    }

    DB::ASTPtr PrepareDNFStatement(const TInclusionStatement& resultStatement)
    {
        DB::ASTs conjuncts;
        for (const auto& tuple : resultStatement.PossibleTuples) {
            DB::ASTs equations;
            for (const auto& [columnName, field] : Zip(resultStatement.ColumnNames, tuple)) {
                auto equationAst = DB::makeASTFunction(
                    "equals",
                    std::make_shared<DB::ASTIdentifier>(columnName),
                    std::make_shared<DB::ASTLiteral>(field));
                equations.emplace_back(std::move(equationAst));
            }
            auto conjunctAst = DB::makeASTForLogicalAnd(std::move(equations));
            conjuncts.emplace_back(conjunctAst);
        }

        auto dnfAst = DB::makeASTForLogicalOr(std::move(conjuncts));

        return dnfAst;
    }

    DB::ASTPtr PrepareConjunctionWithDeductions(DB::ASTFunction& originalAst, TInclusionStatement originalStatement)
    {
        const auto& Logger = Data_.Logger;

        // It may happen that original statement is not implied by any of the deductions, so we always include the original statement.
        DB::ASTs conjunctAsts = {originalAst.clone()};
        for (const auto& entry : Data_.Entries) {
            if (originalStatement.ContainsReferences(entry.References)) {
                auto filteredOriginalStatement = originalStatement.Filter(entry.References);
                auto resultStatement = DeduceComputedColumn(entry, filteredOriginalStatement);

                if (resultStatement.PossibleTuples.empty()) {
                    // We have proven that reference column can't take given values.
                    conjunctAsts.emplace_back(std::make_shared<DB::ASTLiteral>(DB::Field(static_cast<ui8>(0))));
                    continue;
                }

                if (Data_.Settings->DeducedStatementMode == EDeducedStatementMode::In) {
                    conjunctAsts.emplace_back(PrepareInStatement(resultStatement));
                } else {
                    conjunctAsts.emplace_back(PrepareDNFStatement(resultStatement));
                }
            }
        }

        auto conjunctionAst = DB::makeASTForLogicalAnd(std::move(conjunctAsts));
        YT_LOG_TRACE("Query part rewritten (Ast: %v, NewAst: %v)", originalAst, conjunctionAst);

        return conjunctionAst;
    }

    static bool EvaluateConstant(DB::ASTPtr& ast, DB::Field& field, DB::DataTypePtr& dataType, DB::ContextPtr context)
    {
        try {
            std::tie(field, dataType) = DB::evaluateConstantExpression(ast, context);
            return true;
        } catch (const std::exception& ex) {
            return false;
        }
    }

    DB::QueryTreeNodePtr TryRewrite(DB::ASTFunction& ast)
    {
        const auto& Logger = Data_.Logger;

        if (ast.name == "equals") {
            YT_LOG_TRACE("Processing 'equals' (Ast: %v)", ast);
            YT_VERIFY(ast.arguments->children.size() == 2);

            auto lhs = ast.arguments->children[0];
            auto rhs = ast.arguments->children[1];
            // Assume that lhs is an identifier or identifier tuple and rhs is a constant value.
            // If this is not the case, swap lhs and rhs and repeat the procedure.
            for (int swapAttempt = 0; swapAttempt < 2; ++swapAttempt, lhs.swap(rhs)) {
                std::vector<std::string> columnNames;
                bool isLhsTuple = false;
                if (auto* identifierAst = lhs->as<DB::ASTIdentifier>()) {
                    columnNames.emplace_back(identifierAst->shortName());
                } else if (auto maybeColumnNames = IdentifierTupleToColumnNames(*lhs)) {
                    isLhsTuple = true;
                    columnNames = *maybeColumnNames;
                } else {
                    continue;
                }

                YT_LOG_TRACE(
                    "Left-hand corresponds to column names (Lhs: %v, ColumnNames: %v, SwapAttempt: %v)",
                    lhs,
                    columnNames,
                    swapAttempt);

                // Check if expression is constant.
                DB::Field constField;
                DB::DataTypePtr constDataType;

                if (!EvaluateConstant(rhs, constField, constDataType, Data_.getContext())) {
                    YT_LOG_TRACE("Right-hand is non-constant (Rhs: %v, SwapAttempt: %v)", rhs, swapAttempt);
                    continue;
                }
                YT_LOG_TRACE("Right-hand is constant (Rhs: %v, Value: %v, SwapAttempt: %v)", rhs, constField, swapAttempt);

                DB::FieldVector constTuple;

                if (isLhsTuple) {
                    if (constField.getType() == DB::Field::Types::Tuple) {
                        constTuple = constField.safeGet<const DB::Tuple&>();
                    } else {
                        continue;
                    }
                } else {
                    constTuple = {constField};
                }

                if (constTuple.size() != columnNames.size()) {
                    YT_LOG_TRACE(
                        "Left-hand and right-hand have different lengths (Lhs: %v, Rhs: %v)",
                        lhs,
                        rhs);
                    continue;
                }

                return DB::buildQueryTree(
                    PrepareConjunctionWithDeductions(ast, TInclusionStatement(columnNames, {constTuple})),
                    Data_.getContext());
            }

            return nullptr;
        } else if (ast.name == "in") {
            YT_LOG_TRACE("Processing 'in' (Ast: %v)", ast);
            YT_VERIFY(ast.arguments->children.size() == 2);
            auto lhs = ast.arguments->children[0];
            auto rhs = ast.arguments->children[1];
            bool isLhsTuple = false;

            std::vector<std::string> columnNames;
            if (auto* identifierAst = lhs->as<DB::ASTIdentifier>()) {
                columnNames.emplace_back(identifierAst->shortName());
            } else if (auto maybeColumnNames = IdentifierTupleToColumnNames(*lhs)) {
                isLhsTuple = true;
                columnNames = *maybeColumnNames;
            } else {
                return nullptr;
            }

            YT_LOG_TRACE(
                "Left-hand corresponds to column names (Lhs: %v, ColumnNames: %v, IsLhsTuple: %v)",
                lhs,
                columnNames,
                isLhsTuple);

            DB::FieldVector constFields;

            // Check if expression is constant.
            DB::Field constField;
            DB::DataTypePtr constDataType;
            if (!EvaluateConstant(rhs, constField, constDataType, Data_.getContext())) {
                YT_LOG_TRACE("Right-hand is non-constant (Rhs: %v)", rhs);
                return nullptr;
            }
            YT_LOG_TRACE("Right-hand is constant (Rhs: %v, Value: %v)", rhs, constField);

            if (DB::Tuple tuple; constField.tryGet<DB::Tuple>(tuple)) {
                constFields = tuple;
            } else {
                // Assume "key in (42)".
                YT_LOG_TRACE("Right-hand is non-tuple, assuming single-element tuple");
                constFields = {constField};
            }

            std::vector<DB::FieldVector> possibleTuples;

            if (isLhsTuple) {
                for (const auto& constField : constFields) {
                    if (DB::Tuple tuple; constField.tryGet<DB::Tuple>(tuple)) {
                        possibleTuples.emplace_back(tuple);
                    } else {
                        return nullptr;
                    }
                }
            } else {
                possibleTuples = Transpose(constFields);
            }

            for (const auto& tuple : possibleTuples) {
                if (tuple.size() != columnNames.size()) {
                    YT_LOG_TRACE("Right-hand tuple and column names have different sizes (RhsTuple: %v, Columns: %v)", tuple, columnNames);
                    return nullptr;
                }
            }

            return DB::buildQueryTree(
                PrepareConjunctionWithDeductions(ast, TInclusionStatement(columnNames, possibleTuples)),
                Data_.getContext());
        }

        return nullptr;
    }

private:
    Data Data_;
};

DB::ASTPtr PopulatePredicateWithComputedColumns(
    DB::ASTPtr ast,
    const TTableSchemaPtr& schema,
    DB::ContextPtr context,
    DB::PreparedSets& preparedSets,
    const TQuerySettingsPtr& settings,
    NLogging::TLogger logger)
{
    return PopulatePredicateWithComputedColumns(
        DB::buildQueryTree(ast, context),
        schema,
        context,
        preparedSets,
        settings,
        logger
    )->toAST({.add_cast_for_constants = false});
}

DB::QueryTreeNodePtr PopulatePredicateWithComputedColumns(
    DB::QueryTreeNodePtr node,
    const TTableSchemaPtr& schema,
    DB::ContextPtr context,
    DB::PreparedSets& preparedSets,
    const TQuerySettingsPtr& settings,
    NLogging::TLogger logger)
{
    const auto& Logger = logger;

    std::vector<TComputedColumnEntry> entries;

    for (const auto& columnSchema : schema->Columns()) {
        if (columnSchema.Expression() && columnSchema.SortOrder()) {
            THashSet<std::string> referenceSet;
            auto expr = PrepareExpression(*columnSchema.Expression(), *schema, GetBuiltinTypeInferrers(), &referenceSet);
            std::vector<std::string> references(referenceSet.begin(), referenceSet.end());
            entries.emplace_back(TComputedColumnEntry{references, expr, columnSchema.Name(), columnSchema.LogicalType()});
            YT_LOG_DEBUG(
                "Key computed column found (Column: %v, References: %v, Expression: %v)",
                columnSchema.Name(),
                references,
                columnSchema.Expression());
        }
    }

    if (entries.empty()) {
        YT_LOG_DEBUG("Expression has no key computed columns");
        return node;
    }

    auto contextCopy = context;

    TComputedColumnPopulationQueryTreeVisitor::Data data{
        DB::WithContext(context),
        /*Entries*/ std::move(entries),
        /*TableSchema*/ schema,
        /*PreparedSets*/ preparedSets,
        /*Settings*/ settings,
        /*Logger*/ logger
    };
    auto rewrittenNode = node->clone();
    TComputedColumnPopulationQueryTreeVisitor visitor(data);
    visitor.visit(rewrittenNode);

    YT_LOG_DEBUG("Predicate populated with computed column (Ast: %v, NewAst: %v)", node->toAST(), rewrittenNode->toAST());

    return rewrittenNode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
