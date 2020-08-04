#include "computed_columns.h"

#include "schema.h"
#include "helpers.h"
#include "format.h"

#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/folding_profiler.h>
#include <yt/ytlib/query_client/evaluation_helpers.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/makeASTForLogicalFunction.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/TreeRewriter.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

struct TComputedColumnEntry
{
    THashSet<TString> References;
    TConstExpressionPtr Expression;
    TString ComputedColumn;
};

struct TComputedColumnPopulationMatcher
{
    struct Data
    {
        const std::vector<TComputedColumnEntry>& Entries;
        DB::Block& BlockWithConstants;
        const TTableSchemaPtr& TableSchema;
        TLogger Logger;
    };

    static bool needChildVisit(const DB::ASTPtr& node, const DB::ASTPtr& /* child */)
    {
        return !node->as<DB::ASTSubquery>();
    }

    static void visit(DB::ASTPtr& ast, Data& data)
    {
        // Single-term equation case.
        // key == 5 -> (key == 5 AND computed_key == f(5))
        if (auto astFunction = ast->as<DB::ASTFunction>()) {
            if (auto rewrittenAst = visit(*astFunction, data)) {
                ast = rewrittenAst;
                return;
            }
        }
    }

    static DB::ASTPtr DeduceComputedColumn(
        const TComputedColumnEntry& entry,
        const TUnversionedValue& value,
        Data& data)
    {
        const auto& Logger = data.Logger;

        YT_LOG_TRACE("Deducing computed column value (ComputedColumn: %v, Value: %v)", entry.ComputedColumn, value);

        TUnversionedValue result;
        try {
            TCGVariables variables;
            auto callback = Profile(entry.Expression, data.TableSchema->Filter(entry.References), nullptr, &variables)();
            auto rowBuffer = New<TRowBuffer>();
            callback(variables.GetLiteralValues(), variables.GetOpaqueData(), &result, &value, rowBuffer.Get());
            result.Id = 0;
        } catch (const TErrorException& ex) {
            YT_LOG_TRACE(ex, "Caught exception while evaluating expresion");
            // Return false literal. This will result in dropping current equation,
            // which is ok as we deduced that it actually can't take supposed value
            // (YT would not allow writing value which reuslts in exception in computed
            // column computation).
            return std::make_shared<DB::ASTLiteral>(DB::Field(static_cast<ui8>(0)));
        }
        YT_LOG_TRACE("Calculated expression result (ComputedColumnValue: %v)", result);

        auto resultField = ConvertToField(result);
        YT_LOG_TRACE("Converted to CH field (ComputedColumnValue: %v)", resultField.dump());

        auto resultAst = DB::makeASTFunction(
            "equals",
            std::make_shared<DB::ASTIdentifier>(entry.ComputedColumn),
            std::make_shared<DB::ASTLiteral>(resultField));
        YT_LOG_TRACE("Deduced equation (Ast: %v)", resultAst);

        return resultAst;
    }

    static DB::ASTPtr visit(DB::ASTFunction& ast, Data& data)
    {
        const auto& Logger = data.Logger;

        if (ast.name == "equals") {
            YT_LOG_TRACE("Processing equation (Ast: %v)", ast);
            YT_VERIFY(ast.arguments->children.size() == 2);
            std::vector<TString> identifierArgNames;
            std::vector<DB::ASTPtr> otherArgs;

            for (const auto& argumentAst : ast.arguments->children) {
                if (auto* argumentIdentifierAst = argumentAst->as<DB::ASTIdentifier>()) {
                    identifierArgNames.emplace_back(argumentIdentifierAst->shortName());
                } else {
                    otherArgs.emplace_back(argumentAst);
                }
            }
            YT_LOG_TRACE("Equation args (IdentifierArgNames: %v, OtherArgs: %v)", identifierArgNames, otherArgs);
            // Check if equation has form (column) = (const expression).
            if (identifierArgNames.size() != 1) {
                return nullptr;
            }
            YT_VERIFY(otherArgs.size() == 1);
            auto columnName = identifierArgNames.back();
            auto valueAst = otherArgs.back();

            // Check if expression is constant.
            DB::Field constField;
            DB::DataTypePtr constDataType;
            if (!DB::KeyCondition::getConstant(valueAst, data.BlockWithConstants, constField, constDataType)) {
                YT_LOG_TRACE("Expression is non-constant");
                return nullptr;
            }
            YT_LOG_TRACE("Expression is constant (Value: %v)", constField.dump());

            // Convert it to YT unversioned value.
            TUnversionedValue constValue;
            constValue.Id = 0;
            constValue.Type = ToValueType(constField.getType());
            ConvertToUnversionedValue(constField, &constValue);
            YT_LOG_TRACE("Converted to YT unversioned value (Value: %v)", constValue);

            std::vector<DB::ASTPtr> conjunctAsts = {ast.clone()};
            for (const auto& entry : data.Entries) {
                if (entry.References == THashSet<TString>{columnName}) {
                    auto deducedComputedColumn = DeduceComputedColumn(entry, constValue, data);
                    conjunctAsts.emplace_back(std::move(deducedComputedColumn));
                }
            }

            auto conjunctionAst = DB::makeASTForLogicalAnd(std::move(conjunctAsts));
            YT_LOG_TRACE("Equation rewritten (Ast: %v, NewAst: %v)", ast, conjunctionAst);
            return conjunctionAst;
        }

        return nullptr;
    }
};
using TComputedColumnPopulationVisitor = DB::InDepthNodeVisitor<TComputedColumnPopulationMatcher, false>;

DB::ASTPtr PopulatePredicateWithComputedColumns(
    DB::ASTPtr ast,
    const TTableSchemaPtr& schema,
    const DB::Context& context,
    NLogging::TLogger logger)
{
    const auto& Logger = logger;

    std::vector<TComputedColumnEntry> entries;

    for (const auto& columnSchema : schema->Columns()) {
        if (columnSchema.Expression() && columnSchema.SortOrder()) {
            THashSet<TString> references;
            auto expr = PrepareExpression(*columnSchema.Expression(), *schema, BuiltinTypeInferrersMap, &references);
            entries.emplace_back(TComputedColumnEntry{references, expr, columnSchema.Name()});
            YT_LOG_DEBUG(
                "Key computed column found (Column: %v, References: %v, Expression: %v)",
                columnSchema.Name(),
                references,
                columnSchema.Expression());
        }
    }

    if (entries.empty()) {
        YT_LOG_DEBUG("Expression has no key computed columns");
        return ast;
    }

    auto contextCopy = context;
    auto syntaxAnalyzerResult = DB::TreeRewriter(contextCopy).analyze(ast, ToNamesAndTypesList(*schema));
    auto blockWithConstants = DB::KeyCondition::getBlockWithConstants(ast, syntaxAnalyzerResult, contextCopy);
    YT_LOG_TRACE("Block with constants (Block: %v)", blockWithConstants);

    TComputedColumnPopulationMatcher::Data data{
        .Entries = std::move(entries),
        .BlockWithConstants = blockWithConstants,
        .TableSchema = schema,
        .Logger = logger
    };
    auto oldAst = ast->clone();
    TComputedColumnPopulationVisitor(data).visit(ast);

    YT_LOG_DEBUG("Predicates populated with computed column (Ast: %v, NewAst: %v)", oldAst, ast);

    return ast;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
