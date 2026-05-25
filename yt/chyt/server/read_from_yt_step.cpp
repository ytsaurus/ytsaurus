#include "read_from_yt_step.h"

#include <stack>

#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnSet.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/PredicateRewriteVisitor.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAsterisk.h>

#include <Functions/FunctionsMiscellaneous.h>

#include <IO/Operators.h>

namespace DB::Setting {

////////////////////////////////////////////////////////////////////////////////

extern const SettingsBool allow_push_predicate_when_subquery_contains_with;
extern const SettingsBool enable_optimize_predicate_expression_to_final_subquery;
extern const SettingsBool allow_push_predicate_ast_for_distributed_subqueries;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::Setting

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! This is a copy of the logic from the DB::ReadFromRemoteStep, used for the StorageDistributed engine,
//! which tries to convert filters from ActionsDAG back to AST.
DB::ASTPtr TryBuildAdditionalFilterAST(
    const DB::PlannerContextPtr& plannerContext,
    const DB::QueryTreeNodePtr& queryTree,
    const DB::ActionsDAG& dag)
{
    const auto* queryNode = queryTree->as<DB::QueryNode>();
    if (!queryNode) {
        return nullptr;
    }

    std::unordered_set<std::string> projectionNames;
    for (const auto& col : queryNode->getProjectionColumns()) {
        projectionNames.insert(col.name);
    }
    std::unordered_map<std::string, DB::QueryTreeNodePtr> execution_name_to_projection_query_tree;
    for (const auto & node : queryNode->getProjection()) {
        execution_name_to_projection_query_tree[DB::calculateActionNodeName(node, *plannerContext)] = node;
    }

    std::unordered_map<const DB::ActionsDAG::Node*, DB::ASTPtr> nodeToAst;

    struct Frame
    {
        const DB::ActionsDAG::Node* node;
        size_t nextChild = 0;
    };
    std::stack<Frame> stack;
    stack.push({dag.getOutputs().front()});

    while (!stack.empty()) {
        auto& frame = stack.top();
        const auto* node = frame.node;

        if (nodeToAst.contains(node)) {
            stack.pop();
            continue;
        }

        // Labmdas are not supported (converting back to AST is complicated).
        if (DB::WhichDataType(node->result_type).isFunction() ||
                (node->type == DB::ActionsDAG::ActionType::FUNCTION &&
                    typeid_cast<const DB::FunctionCapture*>(node->function_base.get()))) {
            nodeToAst[node] = nullptr;
            stack.pop();
            continue;
        }

        // Support for IN. The stored AST from the Set is taken.
        if (DB::WhichDataType(node->result_type).isSet()) {
            auto maybeSet = node->column;
            if (const auto* colConst = typeid_cast<const DB::ColumnConst*>(maybeSet.get())) {
                maybeSet = colConst->getDataColumnPtr();
            }

            if (const auto* colSet = typeid_cast<const DB::ColumnSet*>(maybeSet.get())) {
                nodeToAst[node] = colSet->getData()->getSourceAST();
            }

            stack.pop();
            continue;
        }

        if (node->column && isColumnConst(*node->column)) {
            auto literal = std::make_shared<DB::ASTLiteral>((*node->column)[0]);
            auto castedLiteral = DB::makeASTFunction(
                "_CAST", literal, std::make_shared<DB::ASTLiteral>(node->result_type->getName()));
            nodeToAst[node] = std::move(castedLiteral);
            stack.pop();
            continue;
        }

        if (frame.nextChild < node->children.size()) {
            stack.push({node->children[frame.nextChild]});
            ++frame.nextChild;
            continue;
        }

        stack.pop();
        auto& res = nodeToAst[node];

        if (node->type == DB::ActionsDAG::ActionType::INPUT) {
            // The column name can be taken from the projection name, or from the projection expression.
            // It depends on the predicate and query stage.
            if (projectionNames.contains(node->result_name)) {
                res = std::make_shared<DB::ASTIdentifier>(node->result_name);
            } else {
                auto it = execution_name_to_projection_query_tree.find(node->result_name);
                if (it != execution_name_to_projection_query_tree.end()) {
                    res = it->second->toAST();
                }
            }
        }

        if (node->type == DB::ActionsDAG::ActionType::ALIAS) {
            res = nodeToAst[node->children.at(0)];
        }

        if (node->type != DB::ActionsDAG::ActionType::FUNCTION) {
            continue;
        }

        if (!node->function_base->isDeterministic() || node->function_base->isStateful()) {
            continue;
        }

        DB::ASTs arguments;
        for (const auto* child : node->children) {
            auto ast = nodeToAst[child];
            if (ast) {
                arguments.push_back(std::move(ast));
            }
        }

        // Allow to skip children only for AND function.
        auto funcName = node->function_base->getName();
        if (funcName == "name") {
            if (arguments.empty()) {
                continue;
            }

            // and() with 1 arg is not allowed. Make it AND(condition, 1).
            if (arguments.size() == 1) {
                arguments.push_back(std::make_shared<DB::ASTLiteral>(DB::Field(1)));
            }
        } else if (arguments.size() != node->children.size()) {
            continue;
        }

        auto function = DB::makeASTFunction(node->function_base->getName(), std::move(arguments));
        res = std::move(function);
    }
    return nodeToAst[dag.getOutputs().front()];
}

void AddFilterToQuery(
    const DB::ContextMutablePtr& context,
    const DB::ASTPtr& queryAst,
    const DB::TableWithColumnNamesAndTypes& tableWithColumns,
    const DB::ASTPtr& pushedDownPredicate)
{
    const auto& settings = context->getSettingsRef();
    if (!settings[DB::Setting::allow_push_predicate_ast_for_distributed_subqueries]) {
        return;
    }

    bool optimizeFinal = settings[DB::Setting::enable_optimize_predicate_expression_to_final_subquery];
    bool optimizeWith = settings[DB::Setting::allow_push_predicate_when_subquery_contains_with];

    DB::ASTs predicates{pushedDownPredicate};
    DB::PredicateRewriteVisitor::Data data(context, predicates, tableWithColumns, optimizeFinal, optimizeWith);

    auto ast = queryAst;
    if (const auto* explain = ast->as<DB::ASTExplainQuery>()) {
        ast = explain->getExplainedQuery();
    }

    data.rewriteSubquery(ast->as<DB::ASTSelectQuery&>(), tableWithColumns.columns.getNames());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TReadFromYTStep::TReadFromYTStep(
    const DB::SelectQueryInfo& queryInfo,
    TDistributedQueryExecutor executor,
    const std::vector<TTablePtr>& tables)
    : DB::SourceStepWithFilterBase(executor.GetOutputHeader())
    , QueryInfo_(queryInfo)
    , IndexStats_(executor.ExtractIndexStats())
    , PrewhereInfo_(queryInfo.prewhere_info)
    , Executor_(std::move(executor))
{
    TStringBuilder description;
    description.AppendString("Tables: [");
    for (const auto& table : tables) {
        description.AppendString(table->GetPath());
        description.AppendString(", ");
    }
    description.AppendString("] ");
    setStepDescription(description.Flush());
}

String TReadFromYTStep::getName() const
{
    return "ReadFromYT";
}

void TReadFromYTStep::initializePipeline(DB::QueryPipelineBuilder& pipeline, const DB::BuildQueryPipelineSettings&)
{
    if (Executor_.PushDownPredicate() && filter_actions_dag) {
        DB::ASTPtr pushedDownPredicate = TryBuildAdditionalFilterAST(
            QueryInfo_.planner_context,
            QueryInfo_.query_tree,
            *filter_actions_dag);

        if (pushedDownPredicate) {
            const auto& context = QueryInfo_.planner_context->getMutableQueryContext();

            const auto* tableNode = QueryInfo_.table_expression->as<DB::TableNode>();
            const auto* tableFunctionNode = QueryInfo_.table_expression->as<DB::TableFunctionNode>();

            DB::DatabaseAndTableWithAlias tableWithAlias;
            DB::StorageSnapshotPtr snapshot;
            if (tableNode) {
                tableWithAlias = DB::DatabaseAndTableWithAlias(tableNode->toASTIdentifier());
                snapshot = tableNode->getStorageSnapshot();
            } else if (tableFunctionNode) {
                snapshot = tableFunctionNode->getStorageSnapshot();
            }

            if (snapshot) {
                DB::TableWithColumnNamesAndTypes tableWithColumns(
                    tableWithAlias,
                    snapshot->getColumns(DB::GetColumnsOptions::Kind::Ordinary));
                tableWithColumns.table.alias = QueryInfo_.table_expression->getAlias();

                Executor_.ModifySecondaryQueries([&] (DB::ASTPtr& query) {
                    AddFilterToQuery(context, query, tableWithColumns, pushedDownPredicate);
                });
            }
        }
    }

    Executor_.Fire();
    auto pipe = Executor_.ExtractUnitedPipe();
    for (const auto& processor : pipe.getProcessors()) {
        processors.emplace_back(processor);
    }

    pipeline.init(std::move(pipe));
}

void TReadFromYTStep::describeIndexes(DB::IQueryPlanStep::FormatSettings& formatSettings) const
{
    if (IndexStats_.empty()) {
        return;
    }
    auto& out = formatSettings.out;

    std::string prefix(formatSettings.offset, formatSettings.indent_char);
    out << prefix << "Indexes: \n";
    for (const auto& indexStat : IndexStats_) {
        indexStat->DescribeIndex(formatSettings);
        out << "\n";
    }
}

void TReadFromYTStep::describeIndexes(DB::JSONBuilder::JSONMap& map) const
{
    if (IndexStats_.empty()) {
        return;
    }
    auto indexesArray = std::make_unique<DB::JSONBuilder::JSONArray>();
    for (const auto& indexStat : IndexStats_) {
        indexesArray->add(indexStat->DescribeIndex());
    }
    map.add("Indexes", std::move(indexesArray));
}

void TReadFromYTStep::describeActions(FormatSettings& formatSettings) const
{
    auto& out = formatSettings.out;

    std::string prefix(formatSettings.offset, formatSettings.indent_char);

    if (PrewhereInfo_) {
        out << prefix << "Prewhere info" << '\n';
        out << prefix << "Need filter: " << PrewhereInfo_->need_filter << '\n';
        out << prefix << "Column: " << PrewhereInfo_->prewhere_column_name;
        if (PrewhereInfo_->remove_prewhere_column) {
            out << " (removed)";
        }
        out << '\n';

        auto expression = std::make_shared<DB::ExpressionActions>(PrewhereInfo_->prewhere_actions.clone());
        expression->describeActions(out, prefix);
    }
}

void TReadFromYTStep::describeActions(DB::JSONBuilder::JSONMap& map) const
{
    if (PrewhereInfo_) {
        auto prewhereInfoMap = std::make_unique<DB::JSONBuilder::JSONMap>();
        prewhereInfoMap->add("Need filter", PrewhereInfo_->need_filter);
        prewhereInfoMap->add("Column", PrewhereInfo_->prewhere_column_name);
        prewhereInfoMap->add("Remove filter column", PrewhereInfo_->remove_prewhere_column);
        auto expression = std::make_shared<DB::ExpressionActions>(PrewhereInfo_->prewhere_actions.clone());
        prewhereInfoMap->add("Prewhere filter expression", expression->toTree());
        map.add("Prewhere info", std::move(prewhereInfoMap));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
