#include "query_analyzer.h"

#include "computed_columns.h"
#include "config.h"
#include "conversion.h"
#include "format.h"
#include "helpers.h"
#include "helpers.h"
#include "host.h"
#include "query_context.h"
#include "query_context.h"
#include "subquery.h"
#include "table.h"

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/createUniqueTableAliases.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Planner/Utils.h>

#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/AggregationUtils.h>

#include <Core/Joins.h>

#include <Planner/Planner.h>
#include <Planner/PlannerJoins.h>

#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Core/InterpolateDescription.h>


#include <DataTypes/DataTypeString.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/misc.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/makeASTForLogicalFunction.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/iterator/enumerate.h>

namespace NYT::NClickHouseServer {

using namespace NChunkPools;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void FillDataSliceDescriptors(
    TSubquerySpec& subquerySpec,
    THashMap<TChunkId, TRefCountedMiscExtPtr> miscExtMap,
    const TRange<NChunkPools::TChunkStripePtr>& chunkStripes)
{
    for (const auto& chunkStripe : chunkStripes) {
        auto& inputDataSliceDescriptors = subquerySpec.DataSliceDescriptors.emplace_back();
        for (const auto& dataSlice : chunkStripe->DataSlices) {
            auto& inputDataSliceDescriptor = inputDataSliceDescriptors.emplace_back();
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                auto& chunkSpec = inputDataSliceDescriptor.ChunkSpecs.emplace_back();
                ToProto(&chunkSpec, chunkSlice, /*comparator*/ TComparator(), EDataSourceType::UnversionedTable);
                auto it = miscExtMap.find(chunkSlice->GetInputChunk()->GetChunkId());
                YT_VERIFY(it != miscExtMap.end());
                if (it->second) {
                    SetProtoExtension(
                        chunkSpec.mutable_chunk_meta()->mutable_extensions(),
                        static_cast<const NChunkClient::NProto::TMiscExt&>(*it->second));
                }
            }
            inputDataSliceDescriptor.VirtualRowIndex = dataSlice->VirtualRowIndex;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Find the longest prefix of key columns from the schema used in keyNode.
int GetUsedKeyPrefixSize(const DB::QueryTreeNodePtr& keyNode, const TTableSchemaPtr& schema)
{
    if (!schema->IsSorted()) {
        return 0;
    }

    THashSet<TString> usedColumns;

    for (int index = 0; index < std::ssize(keyNode->getChildren()); ++index) {
        const auto& node = keyNode->getChildren()[index];
        const auto* columnNode = node->as<DB::ColumnNode>();
        if (columnNode) {
            usedColumns.emplace(columnNode->getColumnName());
        }
    }

    int usedPrefixSize = 0;

    for (int index = 0; index < schema->GetKeyColumnCount(); ++index) {
        const auto& column = schema->Columns()[index];
        if (!usedColumns.contains(column.Name())) {
            break;
        }
        ++usedPrefixSize;
    }

    return usedPrefixSize;
}

//! Create an comparisons expression similar to '<cmpFunc>(<expression>, <literal>)'
//! but the result is correct in terms of YT-comparison (nulls first).
DB::ASTPtr CreateProperNullableComparator(
    const std::string& cmpFunctionName,
    const DB::ASTPtr& leftExpression,
    const DB::Field& rightLiteral)
{
    bool isOrEquals = cmpFunctionName.ends_with("OrEquals");

    bool isRightConstantNull = rightLiteral.isNull();

    if (cmpFunctionName.starts_with("less")) {
        if (isRightConstantNull) {
            if (isOrEquals) {
                // X <= Null -> X is Null
                return DB::makeASTFunction("isNull", leftExpression);
            } else {
                // X < Null -> always false
                return std::make_shared<DB::ASTLiteral>(false);
            }
        } else {
            // X <= CONST -> X is Null or X <= CONST
            // X <  CONST -> X is Null or X <  CONST
            return DB::makeASTFunction(
                "or",
                DB::makeASTFunction("isNull", leftExpression),
                DB::makeASTFunction(
                    cmpFunctionName,
                    leftExpression,
                    std::make_shared<DB::ASTLiteral>(rightLiteral)));
        }
    } else if (cmpFunctionName.starts_with("greater")) {
        if (isRightConstantNull) {
            if (isOrEquals) {
                // X >= Null -> always true
                return std::make_shared<DB::ASTLiteral>(true);
            } else {
                // X > Null -> X is not Null
                return DB::makeASTFunction("isNotNull", leftExpression);
            }
        } else {
            // X >= CONST -> X is not Null and X >= CONST
            // X >  CONST -> X is not Null and X >  CONST
            // But if X is Null, then X > CONST is also Null.
            // Null interpretation in CH is similar to False,
            // so we can simplify the expression to X > CONST
            return DB::makeASTFunction(
                cmpFunctionName,
                leftExpression,
                std::make_shared<DB::ASTLiteral>(rightLiteral));
        }
    } else if (cmpFunctionName == "equals") {
        if (isRightConstantNull) {
            // X == Null -> X is Null
            return DB::makeASTFunction("isNull", leftExpression);
        } else {
            // X == CONST -> X is not Null and X == CONST
            // But, again, it can be simplified to X == CONST (see the explanation above).
            return DB::makeASTFunction(
                cmpFunctionName,
                leftExpression,
                std::make_shared<DB::ASTLiteral>(rightLiteral));
        }
    } else {
        THROW_ERROR_EXCEPTION(
            "Unexpected cmp function name %v; "
            "this is a bug; please, file an issue in CHYT queue",
            cmpFunctionName);
    }
}

DB::ASTPtr CreateKeyComparison(
    std::string cmpFunctionName,
    bool isOrEquals,
    std::vector<DB::ASTPtr> leftExpressions,
    std::vector<DB::Field> rightLiterals,
    bool careAboutNulls)
{
    YT_ASSERT(cmpFunctionName == "less" || cmpFunctionName == "greater");

    YT_VERIFY(leftExpressions.size() == rightLiterals.size());

    int keySize = leftExpressions.size();

    // The key comparison is represented as following:
    // (a, b, c) <= (x, y, z) ->
    // (a < x) or (a == x and b < y) or (a == x and b == y and c <= z)

    // Outer disjunct list: (...) or (...) or (...)
    DB::ASTs disjunctionArgs;
    disjunctionArgs.reserve(keySize);

    // Inner conjunct list: (a == x and b < y)
    // It's modified through iterations by changing comparison function
    // in the last element to 'equals' and adding one more condition at the end.
    // (a == x and b < y) -> (a == x and b == y) -> (a == x and b == y and c <= z)
    DB::ASTs conjunctionArgs;
    conjunctionArgs.reserve(keySize);

    for (int index = 0; index < keySize; ++index) {
        bool isLastElement = (index + 1 == keySize);

        const auto& leftExpression = leftExpressions[index];
        const auto& rightLiteral = rightLiterals[index];

        auto& lastConjunct = conjunctionArgs.emplace_back();

        if (isLastElement && isOrEquals) {
            cmpFunctionName += "OrEquals";
        }

        if (careAboutNulls) {
            lastConjunct = CreateProperNullableComparator(
                cmpFunctionName,
                leftExpression,
                rightLiteral);
        } else {
            lastConjunct = DB::makeASTFunction(
                cmpFunctionName,
                leftExpression,
                std::make_shared<DB::ASTLiteral>(rightLiteral));
        }

        auto conjunctionArgsCopy = conjunctionArgs;
        auto disjunct = DB::makeASTForLogicalAnd(std::move(conjunctionArgsCopy));
        disjunctionArgs.emplace_back(std::move(disjunct));

        // Just to avoid useless extra work.
        if (isLastElement) {
            break;
        }

        // Clone expressions to avoid using it twice in one AST.
        for (auto& conjunct : conjunctionArgs) {
            if (conjunct != lastConjunct) {
                conjunct = conjunct->clone();
            }
        }

        // Replace last inequality with equality.
        // (a == x and b < y) -> (a == x and b == y)
        if (careAboutNulls) {
            lastConjunct = CreateProperNullableComparator(
                "equals",
                leftExpression,
                rightLiteral);
        } else {
            lastConjunct = DB::makeASTFunction(
                "equals",
                leftExpression,
                std::make_shared<DB::ASTLiteral>(rightLiteral));
        }
    }

    return DB::makeASTForLogicalOr(std::move(disjunctionArgs));
}
////////////////////////////////////////////////////////////////////////////////

EReadInOrderMode GetReadInOrderColumnDirection(ESortOrder sortOrder, DB::SortDirection direction)
{
    // Descending sort order is not actually supported in CHYT, but let's prepare for the moment it is.
    switch (sortOrder) {
        case ESortOrder::Ascending:
            return (direction == DB::SortDirection::ASCENDING ? EReadInOrderMode::Forward : EReadInOrderMode::Backward);
        case ESortOrder::Descending:
            return (direction == DB::SortDirection::DESCENDING ? EReadInOrderMode::Forward : EReadInOrderMode::Backward);
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void buildTableExpressionPtrsStackImpl(DB::QueryTreeNodePtr& joinTreeNode, std::vector<DB::QueryTreeNodePtr*>& result)
{
    auto node_type = joinTreeNode->getNodeType();

    switch (node_type)
    {
        case DB::QueryTreeNodeType::TABLE:
        case DB::QueryTreeNodeType::QUERY:
        case DB::QueryTreeNodeType::UNION:
        case DB::QueryTreeNodeType::TABLE_FUNCTION: {
            result.push_back(&joinTreeNode);
            break;
        }
        case DB::QueryTreeNodeType::ARRAY_JOIN: {
            auto & array_join_node = joinTreeNode->as<DB::ArrayJoinNode&>();
            buildTableExpressionPtrsStackImpl(array_join_node.getTableExpression(), result);
            result.push_back(&joinTreeNode);
            break;
        }
        case DB::QueryTreeNodeType::JOIN: {
            auto & join_node = joinTreeNode->as<DB::JoinNode&>();
            buildTableExpressionPtrsStackImpl(join_node.getLeftTableExpression(), result);
            buildTableExpressionPtrsStackImpl(join_node.getRightTableExpression(), result);
            result.push_back(std::move(&joinTreeNode));
            break;
        }
        default: {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
                "Unexpected node type for table expression. Expected table, table function, query, union, join or array join. Actual {}",
                joinTreeNode->getNodeTypeName());
        }
    }
}

std::vector<DB::QueryTreeNodePtr*> buildTableExpressionPtrsStack(DB::QueryTreeNodePtr& joinTreeNode)
{
    std::vector<DB::QueryTreeNodePtr*> result;
    buildTableExpressionPtrsStackImpl(joinTreeNode, result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct JoinKeyLists
{
    std::vector<DB::QueryTreeNodePtr> joinLeftKeys;
    std::vector<DB::QueryTreeNodePtr> joinRightKeys;

    void addNode(DB::JoinTableSide side, DB::QueryTreeNodePtr node) {
        auto& joinKeys = (side == DB::JoinTableSide::Left) ? joinLeftKeys : joinRightKeys;
        joinKeys.emplace_back(std::move(node));
    }
};

JoinKeyLists ParseJoinKeyColumns(const DB::QueryTreeNodePtr& queryNode, const DB::PlannerContextPtr& plannerContext)
{
    JoinKeyLists lists;

    const auto& joinNode = queryNode->as<DB::JoinNode&>();
    auto expressionNode = joinNode.getJoinExpression();

    if (joinNode.isUsingJoinExpression()) {
        auto & usingList = expressionNode->as<DB::ListNode&>();
        for (auto & usingNode : usingList.getNodes()) {
            auto& usingColumnNode = usingNode->as<DB::ColumnNode&>();
            auto& using_join_columns_list = usingColumnNode.getExpressionOrThrow()->as<DB::ListNode&>();
            auto& leftJoinColumnNode = using_join_columns_list.getNodes().at(0);
            auto& rightJoinColumnNode = using_join_columns_list.getNodes().at(1);
            lists.joinLeftKeys.push_back(leftJoinColumnNode);
            lists.joinRightKeys.push_back(rightJoinColumnNode);
        }
    } else {
        auto joinClauses = DB::buildJoinClausesAndActions(
            {}, {}, queryNode, plannerContext).join_clauses;

        for (const auto& clause : joinClauses) {
            const auto& leftExpressions = clause.getLeftKeyExpressionNodes();
            const auto& rightExpressions = clause.getRightKeyExpressionNodes();
            lists.joinLeftKeys.insert(lists.joinLeftKeys.end(), leftExpressions.begin(), leftExpressions.end());
            lists.joinRightKeys.insert(lists.joinRightKeys.end(), rightExpressions.begin(), rightExpressions.end());
        }
    }

    return lists;
}

////////////////////////////////////////////////////////////////////////////////

TQueryAnalyzer::TQueryAnalyzer(
    DB::ContextPtr context,
    const TStorageContext* storageContext,
    const DB::SelectQueryInfo& queryInfo,
    const TLogger& logger)
    : DB::WithContext(std::move(context))
    , StorageContext_(storageContext)
    , QueryInfo_(queryInfo)
    , Logger(logger)
{
    // When allow_experimental_analyzer = 0, SelectQueryInfo does not contain a query_tree.
    // Therefore, we initialize it ourselves.
    if (!QueryInfo_.query_tree) {
        auto selectQueryOptions = DB::SelectQueryOptions().analyze();
        QueryInfo_.query_tree = DB::InterpreterSelectQueryAnalyzer(
            QueryInfo_.query,
            getContext(),
            selectQueryOptions).getPlanner().buildSelectQueryInfo().query_tree;
    }
    ParseQuery();
}

void TQueryAnalyzer::InferSortedJoinKeyColumns(bool needSortedPool)
{
    YT_VERIFY(Join_ && !CrossJoin_ && Storages_.size() == 2);

    const auto& leftStorage = Storages_[0];
    const auto& rightStorage = Storages_[1];

    const auto& leftTableSchema = *leftStorage->GetSchema();
    const auto& rightTableSchema = (rightStorage ? *rightStorage->GetSchema() : TTableSchema());

    auto* queryNode = QueryInfo_.query_tree->as<DB::QueryNode>();
    YT_VERIFY(queryNode);
    auto joinNodeType = queryNode->getJoinTree()->getNodeType();
    YT_VERIFY(joinNodeType == DB::QueryTreeNodeType::JOIN || joinNodeType == DB::QueryTreeNodeType::ARRAY_JOIN);

    auto joinNode = queryNode->getJoinTree();
    if (joinNodeType == DB::QueryTreeNodeType::ARRAY_JOIN) {
        // In the case where the outermost join in a JoinTree is an ArrayJoin,
        // it should be skipped because further analysis is performed for a normal join.
        // We can safely do this because there can be at most one ArrayJoin in a JoinTree
        // and we know that the JoinTree contains a normal join.
        joinNode = joinNode->as<DB::ArrayJoinNode>()->getTableExpression();
    }

    auto keyLists = ParseJoinKeyColumns(joinNode, QueryInfo_.planner_context);
    const auto& joinLeftKeys = keyLists.joinLeftKeys;
    const auto& joinRightKeys = keyLists.joinRightKeys;

    // This condition fails sometimes in multiple join due to a bug in ClickHouse.
    // https://github.com/ClickHouse/ClickHouse/issues/29734
    // CHYT-679
    // Throw an exception instead of YT_VERIFY to avoid a clique crash until the bug is fixed.
    // YT_VERIFY(joinLeftKeys.size() == joinRightKeys.size());
    if (joinLeftKeys.size() != joinRightKeys.size()) {
        THROW_ERROR_EXCEPTION("Unexpected join condition; see CHYT-679 for more details");
    }

    int joinKeySize = joinLeftKeys.size();

    auto getKeyColumnToPositionMap = [] (const TTableSchema& tableSchema) {
        THashMap<TString, int> nameToPosition;

        for (int index = 0; index < tableSchema.GetKeyColumnCount(); ++index) {
            const auto& column = tableSchema.Columns()[index];
            auto [_, inserted] = nameToPosition.emplace(column.Name(), index);
            YT_ASSERT(inserted);
        }
        return nameToPosition;
    };

    auto leftKeyPositionMap = getKeyColumnToPositionMap(leftTableSchema);
    auto rightKeyPositionMap = getKeyColumnToPositionMap(rightTableSchema);

    auto getKeyColumnPosition = [] (const THashMap<TString, int>& positionMap, const TString& name) {
        auto it = positionMap.find(name);
        return (it == positionMap.end()) ? -1 : it->second;
    };

    auto checkTableSorted = [&] (int tableIndex) {
        const auto& storage = Storages_[tableIndex];
        YT_VERIFY(storage);
        const auto& tableExpression = TableExpressions_[tableIndex];
        const auto& underlyingTables = storage->GetTables();

        const char* errorPrefix = (TwoYTTableJoin_? "Invalid sorted JOIN" : "Invalid RIGHT or FULL JOIN");

        if (underlyingTables.size() != 1) {
            THROW_ERROR_EXCEPTION(
                "%v: joining concatenation of multiple tables is not supported; "
                "you can suppress this error at cost of possible performance degradation "
                "by wrapping the table into subquery",
                errorPrefix)
                << TErrorAttribute("table_expression", tableExpression->formatASTForErrorMessage())
                << TErrorAttribute("table_index", tableIndex)
                << TErrorAttribute("underlying_table_count", underlyingTables.size())
                << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/data-processing/chyt/queries/joins");
        }

        const auto& path = underlyingTables.front()->Path;
        const auto& schema = *storage->GetSchema();

        if (!schema.IsSorted()) {
            THROW_ERROR_EXCEPTION(
                "%v: table %Qv is not sorted; "
                "you can suppress this error at cost of possible performance degradation "
                "by wrapping the table into subquery",
                errorPrefix,
                path)
                << TErrorAttribute("table_index", tableIndex)
                << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/data-processing/chyt/queries/joins");
        }
    };

    THashSet<TString> matchedLeftKeyNames;
    // For better error messages.
    std::vector<std::pair<DB::ASTPtr, DB::ASTPtr>> unmatchedKeyPairs;
    // Map for every key column from the left table schema to corresponding key expression from right table expression.
    // Make sense only when joined table is not YT-table.
    std::vector<DB::QueryTreeNodePtr> joinKeyExpressions(leftTableSchema.GetKeyColumnCount());

    if (TwoYTTableJoin_) {
        // Two YT table join always requires sorted pool.
        YT_VERIFY(needSortedPool);

        // Both tables should be sorted and simple (concatYtTables is forbidden).
        checkTableSorted(/*tableIndex*/ 0);
        checkTableSorted(/*tableIndex*/ 1);

        for (int index = 0; index < joinKeySize; ++index) {
            const auto* leftKeyColumn = joinLeftKeys[index]->as<DB::ColumnNode>();
            const auto* rightKeyColumn = joinRightKeys[index]->as<DB::ColumnNode>();

            // Cannot match expressions.
            if (!leftKeyColumn || !rightKeyColumn) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index]->toAST(), joinRightKeys[index]->toAST());
                continue;
            }

            int leftKeyPosition = getKeyColumnPosition(leftKeyPositionMap, TString(leftKeyColumn->getColumnName()));
            int rightKeyPosition = getKeyColumnPosition(rightKeyPositionMap, TString(rightKeyColumn->getColumnName()));

            // Cannot match keys in different positions.
            if (leftKeyPosition == -1 || rightKeyPosition == -1 || leftKeyPosition != rightKeyPosition) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index]->toAST(), joinRightKeys[index]->toAST());
                continue;
            }

            matchedLeftKeyNames.emplace(leftKeyColumn->getColumnName());
        }
    } else {
        bool leftTableSorted = leftTableSchema.IsSorted() && (leftStorage->GetTables().size() == 1);

        // If sorted pool is not necessary, prevent throwing an error.
        // Otherwise, checkTableSorted will produce proper error.
        if (!leftTableSorted && !needSortedPool) {
            return;
        }
        checkTableSorted(/*tableIndex*/ 0);

        for (int index = 0; index < joinKeySize; ++index) {
            const auto* leftKeyColumn = joinLeftKeys[index]->as<DB::ColumnNode>();
            // TODO(dakovalkov): Check that expression is deterministic.
            const auto& rightKeyExpression = joinRightKeys[index];

            if (!leftKeyColumn) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index]->toAST(), joinRightKeys[index]->toAST());
                continue;
            }

            int keyPosition = getKeyColumnPosition(leftKeyPositionMap, TString(leftKeyColumn->getColumnName()));
            // Not a key column, ignore it.
            if (keyPosition == -1) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index]->toAST(), joinRightKeys[index]->toAST());
                continue;
            }

            matchedLeftKeyNames.emplace(leftKeyColumn->getColumnName());

            joinKeyExpressions[keyPosition] = rightKeyExpression;
        }
    }

    int matchedKeyPrefixSize = 0;

    for (int index = 0; index < leftTableSchema.GetKeyColumnCount(); ++index) {
        const auto& column = leftTableSchema.Columns()[index];
        if (!matchedLeftKeyNames.contains(column.Name())) {
            break;
        }
        ++matchedKeyPrefixSize;
    }

    if (matchedKeyPrefixSize == 0) {
        // Prevent error when sored pool is not required.
        if (!needSortedPool) {
            return;
        }

        const char* errorPrefix = (TwoYTTableJoin_ ? "Invalid sorted JOIN" : "Invalid RIGHT or FULL JOIN");
        THROW_ERROR_EXCEPTION("%v: key is empty", errorPrefix)
            << TErrorAttribute("matched_left_key_columns", matchedLeftKeyNames)
            << TErrorAttribute("left_key_position_map", leftKeyPositionMap)
            << TErrorAttribute("right_key_position_map", rightKeyPositionMap)
            << TErrorAttribute("unmatched_key_pairs", unmatchedKeyPairs)
            << TErrorAttribute("join_key_size", joinKeySize);
    }

    KeyColumnCount_ = matchedKeyPrefixSize;
    JoinedByKeyColumns_ = true;

    // Save key expressions from joined table in order to use it for creating bound conditions later.
    if (!TwoYTTableJoin_) {
        joinKeyExpressions.resize(matchedKeyPrefixSize);
        JoinKeyRightExpressions_ = std::move(joinKeyExpressions);
    }
}

class TCheckInFunctionExistsVisitor
    : public DB::InDepthQueryTreeVisitor<TCheckInFunctionExistsVisitor>
{
public:
    TCheckInFunctionExistsVisitor(const DB::QueryTreeNodePtr& joinTree)
        : JoinTree_(joinTree)
    { }

    void visitImpl(const DB::QueryTreeNodePtr& node)
    {
        auto* functionNode = node->as<DB::FunctionNode>();
        if (!functionNode) {
            return;
        }

        if (!DB::functionIsInOrGlobalInOperator(functionNode->getFunctionName())) {
            return;
        }
        HasInOperator_ = true;

        // CH deduplicates QueryTree nodes, making pointers to a single QueryTreeNode.
        // In case of IN function for secondary queries to work correctly, we must duplicate them back.
        for (auto& arg : functionNode->getArguments()) {
            if (DB::isNodePartOfTree(arg.get(), JoinTree_.get())) {
                arg = arg->clone();
            }
        }
    }

    bool needChildVisit(const DB::QueryTreeNodePtr&, const DB::QueryTreeNodePtr& childNode) const
    {
        if (HasInOperator_) {
            return false;
        }

        auto childNodeType = childNode->getNodeType();
        return !(childNodeType == DB::QueryTreeNodeType::QUERY || childNodeType == DB::QueryTreeNodeType::UNION);
    }

    bool HasInOperator() const
    {
        return HasInOperator_;
    }

private:
    const DB::QueryTreeNodePtr& JoinTree_;
    bool HasInOperator_ = false;
};

void TQueryAnalyzer::ParseQuery()
{
    auto* selectQuery = QueryInfo_.query_tree->as<DB::QueryNode>();
    YT_VERIFY(selectQuery);

    YT_LOG_DEBUG("Analyzing query (Query: %v)", selectQuery->toAST());

    YT_VERIFY(selectQuery->getJoinTree());

    TCheckInFunctionExistsVisitor visitor(selectQuery->getJoinTree());
    visitor.visit(QueryInfo_.query_tree);
    HasInOperator_ = visitor.HasInOperator();

    auto tableExpressionsStack = buildTableExpressionPtrsStack(selectQuery->getJoinTree());
    for (int index = 0; index < std::ssize(tableExpressionsStack); ++index) {
        auto tableExpressionPtr = tableExpressionsStack[index];
        auto tableExpression = *tableExpressionPtr;
        auto tableExpressionNodeType = tableExpression->getNodeType();

        if (tableExpressionNodeType == DB::QueryTreeNodeType::ARRAY_JOIN) {
            // First element should always be a table expression.
            YT_VERIFY(index != 0);
            // ArrayJoin is always the last node in a JoinTree.
            // Therefore, we can break the loop at this point.
            break;
        }

        if (tableExpressionNodeType == DB::QueryTreeNodeType::JOIN) {
            Join_ = true;
            const auto& joinNode = tableExpression->as<DB::JoinNode&>();
            if (joinNode.getLocality() == DB::JoinLocality::Global) {
                YT_LOG_DEBUG("Table expression is a global join (Index: %v)", index);
                GlobalJoin_ = true;
            }
            auto joinKind = joinNode.getKind();
            if (joinKind == DB::JoinKind::Right || joinKind == DB::JoinKind::Full) {
                YT_LOG_DEBUG("Query is a right or full join");
                RightOrFullJoin_ = true;
            }
            if (joinKind == DB::JoinKind::Cross) {
                YT_LOG_DEBUG("Query is a cross join");
                CrossJoin_ = true;
            }

            // When distributing requests, we can only affect the execution of the first join,
            // so after it, the other parts in the JoinTree can be ignored.
            break;
        }

        TableExpressions_.emplace_back(tableExpression);
        TableExpressionPtrs_.emplace_back(tableExpressionPtr);
    }

    YT_VERIFY(TableExpressions_.size() >= 1);
    YT_VERIFY(TableExpressions_.size() <= 2);
    for (size_t tableExpressionIndex = 0; tableExpressionIndex < TableExpressions_.size(); ++tableExpressionIndex) {
        const auto& tableExpression = TableExpressions_[tableExpressionIndex];

        auto& storage = Storages_.emplace_back(GetStorage(tableExpression));
        if (storage) {
            YT_LOG_DEBUG("Table expression corresponds to TStorageDistributor (TableExpression: %v)",
                tableExpression->toAST());
            ++YtTableCount_;
        } else {
            YT_LOG_DEBUG("Table expression does not correspond to TStorageDistributor (TableExpression: %v)",
                tableExpression->toAST());
        }
    }

    YT_VERIFY(YtTableCount_ > 0);

    if (YtTableCount_ == 2) {
        if (!CrossJoin_) {
            YT_LOG_DEBUG("Query is a two-YT-table join");
            TwoYTTableJoin_ = true;
        } else {
            YT_LOG_DEBUG("Query is a two-YT-table cross join; considering this as a single YT table join");
            YtTableCount_ = 1;
            TwoYTTableJoin_ = false;
            TableExpressions_.pop_back();
            TableExpressionPtrs_.pop_back();
            Storages_.pop_back();
        }
    }

    YT_LOG_DEBUG(
        "Extracted table expressions from query (Query: %v, TableExpressionCount: %v, YtTableCount: %v, "
        "IsJoin: %v, IsGlobalJoin: %v, IsRightOrFullJoin: %v, IsCrossJoin: %v)",
        *QueryInfo_.query,
        TableExpressions_.size(),
        YtTableCount_,
        Join_,
        GlobalJoin_,
        RightOrFullJoin_,
        CrossJoin_);
}

DB::QueryProcessingStage::Enum TQueryAnalyzer::GetOptimizedQueryProcessingStage() const
{
    if (!Prepared_) {
        THROW_ERROR_EXCEPTION("Query analyzer is not prepared but GetOptimizedQueryProcessingStage method is already called; "
            "this is a bug; please, file an issue in CHYT queue");
    }

    if (!OptimizedQueryProcessingStage_) {
        const auto& settings = StorageContext_->Settings;
        if (!settings->Execution->OptimizeQueryProcessingStage) {
            THROW_ERROR_EXCEPTION(
                "setting chyt.execution.optimize_query_processing_stage is not set "
                "but we're trying to optimize stage; "
                "this is a bug; please, file an issue in CHYT queue");
        }
        THROW_ERROR_EXCEPTION(
            "Unexpected call to get optimized query processing stage; "
            "this is a bug; please, file an issue in CHYT queue");
    }
    return *OptimizedQueryProcessingStage_;
}

EReadInOrderMode TQueryAnalyzer::GetReadInOrderMode() const
{
    if (!Prepared_) {
        THROW_ERROR_EXCEPTION("Query analyzer is not prepared, but GetReadInOrderMode method is already called; "
            "this is a bug; please, file an issue in the relevant queue");
    }

    return ReadInOrderMode_;
}

void TQueryAnalyzer::OptimizeQueryProcessingStage()
{
    const auto& settings = StorageContext_->Settings;

    const auto& queryNode = QueryInfo_.query_tree->as<DB::QueryNode&>();
    const auto& storage = Storages_[0];

    bool allowPoolSwitch = settings->Execution->AllowSwitchToSortedPool;
    bool allowKeyCut = settings->Execution->AllowKeyTruncating;

    // We can always execute query up to WithMergeableState.
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::WithMergeableState;

    // Some checks from native CH routine.
    if (queryNode.isGroupByWithTotals() || queryNode.isGroupByWithRollup() || queryNode.isGroupByWithCube()) {
        return;
    }
    if (DB::hasWindowFunctionNodes(QueryInfo_.query_tree)) {
        return;
    }
    if (getContext()->getSettingsRef().extremes) {
        return;
    }

    bool isSingleTable = (storage->GetTables().size() == 1);
    int keyColumnCount = KeyColumnCount_;

    auto processAggregationKeyNode = [&] (const DB::QueryTreeNodePtr& keyNode) {
        // SortedPool expects non-overlapping sorted chunks. In case of multiple
        // tables (concatYtTablesRange), this condition is broken,
        // so we cannot use SortedPool to optimize aggregation.
        // TODO(dakovalkov): Theoretically, the optimization is still possible,
        // but we need to overthink SortedPool logic.
        if (!isSingleTable) {
            return false;
        }

        int usedKeyColumnCount = GetUsedKeyPrefixSize(keyNode, storage->GetSchema());

        bool canOptimize = (usedKeyColumnCount > 0)
            && (keyColumnCount > 0 || allowPoolSwitch)
            && (keyColumnCount <= usedKeyColumnCount || allowKeyCut);

        if (canOptimize) {
            if (keyColumnCount) {
                keyColumnCount = std::min(keyColumnCount, usedKeyColumnCount);
            } else {
                keyColumnCount = usedKeyColumnCount;
            }
        }

        return canOptimize;
    };

    bool hasAggregates = hasAggregateFunctionNodes(QueryInfo_.query_tree);
    if (QueryInfo_.syntax_analyzer_result) {
        hasAggregates = hasAggregates || !QueryInfo_.syntax_analyzer_result->aggregates.empty();
    }

    // Simple aggregation without groupBy, e.g. 'select avg(x) from table'.
    if (hasAggregates && !queryNode.hasGroupBy()) {
        return;
    }
    if (queryNode.hasGroupBy() && !processAggregationKeyNode(queryNode.getGroupByNode())) {
        return;
    }
    if (queryNode.isDistinct() && !processAggregationKeyNode(queryNode.getProjectionNode())) {
        return;
    }
    if (queryNode.hasLimitBy() && !processAggregationKeyNode(queryNode.getLimitByNode())) {
        return;
    }

    // All aggregations can be performed on instances, switching to 'AfterAggregation' stage.
    YT_VERIFY(KeyColumnCount_ == 0 || keyColumnCount <= KeyColumnCount_);
    KeyColumnCount_ = keyColumnCount;
    if (!JoinKeyRightExpressions_.empty()) {
        JoinKeyRightExpressions_.resize(KeyColumnCount_);
    }
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    if (queryNode.hasLimit() || queryNode.hasOffset()) {
        return;
    }
    if (queryNode.hasOrderBy()) {
        // TODO(dakovalkov): We can also analyze orderBy node.
        // This information won't help to optimize query processing stage,
        // but it could be used to perform distributed insert into sorted table.
        return;
    }

    // Query does not contain Limit/OrderBy clauses, the result is complete.
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::Complete;
}

void TQueryAnalyzer::InferReadInOrderMode(bool assumeNoNullKeys, bool assumeNoNanKeys)
{
    auto* queryNode = QueryInfo_.query_tree->as<DB::QueryNode>();

    // Read in order is forbidden in JOINs and queries with aggregation.
    // It might be useful in some of these cases, but that is a question for another day.
    if (Join_ || queryNode->hasGroupBy() || queryNode->hasHaving() || queryNode->hasWindow() || queryNode->hasLimitBy()) {
        return;
    }

    // Read in order makes no sense without requested order.
    // Native CH optimizations will do the trick.
    if (!queryNode->hasOrderBy()) {
        return;
    }

    // Read in order makes no sense without any limits specified.
    // The whole table will probably be read anyway.
    if (!queryNode->hasLimit() && !getContext()->getSettingsRef().limit) {
        return;
    }

    YT_VERIFY(YtTableCount_ == 1);
    const auto& schema = Storages_[0]->GetSchema();

    // If the underlying table is not sorted we cannot help in any way.
    if (!schema->IsSorted()) {
        return;
    }

    auto commonDirection = EReadInOrderMode::None;

    // Columns from the ORDER BY clause must form a prefix of the table's primary key.
    if (std::ssize(queryNode->getOrderBy().getNodes()) > schema->GetKeyColumnCount()) {
        return;
    }

    for (const auto& [columnIndex, orderByElementNode] : Enumerate(queryNode->getOrderBy().getNodes())) {
        auto orderBySortNode = orderByElementNode->as<DB::SortNode>();

        auto orderByColumnNode = orderBySortNode->getExpression()->as<DB::ColumnNode>();
        // Functions are not supported. It is possible, but I do not think there is much use.
        if (!orderByColumnNode) {
            return;
        }

        // Correct index range and sort order presence is guaranteed by the key column count above.
        const auto& schemaColumn = schema->Columns()[columnIndex];
        YT_VERIFY(schemaColumn.SortOrder());

        // Columns from the ORDER BY clause must form a prefix of the table's primary key.
        if (orderByColumnNode->getColumnName() != schemaColumn.Name()) {
            return;
        }

        // All requested directions must align and form either a forward or backward read of the underlying table.
        auto columnDirection = GetReadInOrderColumnDirection(*schemaColumn.SortOrder(), orderBySortNode->getSortDirection());
        if (commonDirection == EReadInOrderMode::None) {
            commonDirection = columnDirection;
        } else if (commonDirection != columnDirection) {
            return;
        }

        // In YT, NULL values compare less than any other values. In ClickHouse, NULL value placement
        // depends solely on the placement requested in the query, which defaults to NULLS LAST.
        // Since we combine both YT and CH sorting in this mode, there is only one acceptable direction
        // depending on the requested sort order. Otherwise, we cannot use this mode, except in cases
        // when we know that no NULLS are present. We check all of this below.

        bool isFloat = IsFloatingPointType(schemaColumn.CastToV1Type());
        bool isRequired = schemaColumn.Required();

        bool couldHaveNulls = !isRequired && !assumeNoNullKeys;
        bool couldHaveNans = isFloat && !assumeNoNanKeys;

        // Sadly, with both NANs and NULLs we are just screwed. In YT NAN values are larger than any others,
        // but in CHYT they are always placed together with NULLs. These two approaches contradict each other,
        // so we cannot optimize.
        if (couldHaveNulls && couldHaveNans) {
            return;
        }

        if (!couldHaveNulls && !couldHaveNans) {
            continue;
        }

        // The value of nulls_direction is equal to direction for NULLS LAST and to the opposite
        // of direction for NULLS FIRST.
        auto nullsDirection = orderBySortNode->getNullsSortDirection().value_or(orderBySortNode->getSortDirection());

        // For ASC (d = 1), we need NULLS FIRST (nd = -d = -1).
        // For DSC (d = -1), we need NULLS LAST (nd =  d = -1).
        if (couldHaveNulls && nullsDirection != DB::SortDirection::DESCENDING) {
            return;
        }

        // For ASC (d = 1), we need NANS LAST => NULLS LAST    (nd =   d = 1).
        // For DSC (d = -1), we need NANS FIRST => NULLS FIRST (nd =  -d = 1).
        if (couldHaveNans && nullsDirection != DB::SortDirection::ASCENDING) {
            return;
        }
    }

    ReadInOrderMode_ = commonDirection;
}

TQueryAnalysisResult TQueryAnalyzer::Analyze() const
{
    if (!Prepared_) {
        THROW_ERROR_EXCEPTION("Query analyzer is not prepared but Analyze method is already called; "
            "this is a bug; please, file an issue in CHYT queue");
    }

    const auto& settings = StorageContext_->Settings;

    TQueryAnalysisResult result;

    for (const auto& [index, storage] : Enumerate(Storages_)) {
        if (!storage) {
            continue;
        }
        result.Tables.emplace_back(storage->GetTables());
        auto schema = storage->GetSchema();
        std::optional<DB::KeyCondition> keyCondition;
        if (schema->IsSorted()) {
            auto primaryKeyExpression = std::make_shared<DB::ExpressionActions>(DB::ActionsDAG(
                ToNamesAndTypesList(*schema, settings->Composite)));

            auto* selectQuery = QueryInfo_.query_tree->as<DB::QueryNode>();
            YT_VERIFY(selectQuery);

            std::shared_ptr<const DB::ActionsDAG> filterActionsDAG;
            if (settings->EnableComputedColumnDeduction && selectQuery->hasWhere()) {
                // Query may not contain deducible values for computed columns.
                // We populate query with deducible equations on computed columns,
                // so key condition is able to properly filter ranges with computed
                // key columns.
                auto modifiedWhere = PopulatePredicateWithComputedColumns(
                    selectQuery->getWhere(),
                    schema,
                    getContext(),
                    *QueryInfo_.prepared_sets,
                    settings,
                    Logger);

                // modifiedWhere contains unresolved identifiers only for the currently processed table expression.
                // Therefore, we use this table expression as a source for query analysis.
                DB::QueryAnalysisPass query_analysis_pass(TableExpressions_[index]);
                query_analysis_pass.run(modifiedWhere, getContext());

                auto prevWhere = std::move(selectQuery->getWhere());
                selectQuery->getWhere() = modifiedWhere;

                // During construction Planner collects query filters to analyze.
                // Since we changed the query, we need to get new filters to create a KeyCondition with calculated columns.
                DB::SelectQueryOptions options;
                DB::Planner planner(QueryInfo_.query_tree, options);
                const auto & table_filters = planner.getPlannerContext()->getGlobalPlannerContext()->filters_for_table_expressions;
                auto it = table_filters.find(TableExpressions_[index]);
                if (it != table_filters.end()) {
                    const auto& filters = it->second;
                    // TODO (buyval01) : investigate adding prewhere filters to the common filterActionsDAG.
                    filterActionsDAG = std::make_shared<const DB::ActionsDAG>(filters.filter_actions->clone());
                }

                selectQuery->getWhere() = std::move(prevWhere);
            } else {
                const auto& tableExpressionData = QueryInfo_.planner_context->getTableExpressionDataOrThrow(TableExpressions_[index]);
                if (const auto& filterActions = tableExpressionData.getFilterActions()) {
                    filterActionsDAG = std::make_shared<const DB::ActionsDAG>(filterActions->clone());
                }
            }

            keyCondition.emplace(filterActionsDAG.get(), getContext(), schema->GetKeyColumns(), primaryKeyExpression);
        }
        result.KeyConditions.emplace_back(std::move(keyCondition));
        result.TableSchemas.emplace_back(storage->GetSchema());
    }

    result.ReadInOrderMode = ReadInOrderMode_;

    if (ReadInOrderMode_ != EReadInOrderMode::None) {
        result.PoolKind = EPoolKind::Sorted;
        result.KeyColumnCount = Storages_[0]->GetSchema()->GetKeyColumnCount();
    } else {
        result.PoolKind = (KeyColumnCount_ > 0 ? EPoolKind::Sorted : EPoolKind::Unordered);
        result.KeyColumnCount = KeyColumnCount_;
    }

    return result;
}

TSecondaryQuery TQueryAnalyzer::CreateSecondaryQuery(
    const TRange<TSubquery>& threadSubqueries,
    TSubquerySpec specTemplate,
    const THashMap<TChunkId, TRefCountedMiscExtPtr>& miscExtMap,
    int subqueryIndex,
    bool isLastSubquery)
{
    if (!Prepared_) {
        THROW_ERROR_EXCEPTION("Query analyzer is not prepared but CreateSecondaryQuery method is already called; "
            "this is a bug; please, file an issue in CHYT queue");
    }

    auto Logger = this->Logger.WithTag("SubqueryIndex: %v", subqueryIndex);

    i64 totalRowCount = 0;
    i64 totalDataWeight = 0;
    i64 totalChunkCount = 0;
    for (const auto& subquery : threadSubqueries) {
        const auto& stripeList = subquery.StripeList;
        totalRowCount += stripeList->TotalRowCount;
        totalDataWeight += stripeList->TotalDataWeight;
        totalChunkCount += stripeList->TotalChunkCount;
    }

    YT_LOG_DEBUG(
        "Rewriting query (YtTableCount: %v, ThreadSubqueryCount: %v, TotalDataWeight: %v, TotalRowCount: %v, TotalChunkCount: %v)",
        YtTableCount_,
        threadSubqueries.size(),
        totalDataWeight,
        totalRowCount,
        totalChunkCount);

    specTemplate.SubqueryIndex = subqueryIndex;

    std::vector<DB::QueryTreeNodePtr> newTableExpressions;

    DB::Scalars scalars;

    for (int index = 0; index < YtTableCount_; ++index) {
        auto tableExpressionNode = TableExpressions_[index];

        std::vector<TChunkStripePtr> stripes;
        for (const auto& subquery : threadSubqueries) {
            stripes.emplace_back(subquery.StripeList->Stripes[index]);
        }

        auto spec = specTemplate;
        spec.TableIndex = index;
        spec.ReadSchema = Storages_[index]->GetSchema();

        FillDataSliceDescriptors(spec, miscExtMap, TRange(stripes));

        auto protoSpec = NYT::ToProto<NProto::TSubquerySpec>(spec);
        auto encodedSpec = protoSpec.SerializeAsString();

        YT_LOG_DEBUG("Serializing subquery spec (TableIndex: %v, SpecLength: %v)", index, encodedSpec.size());

        std::string scalarName = "yt_table_" + std::to_string(index);

        auto scalarNode = std::make_shared<DB::ConstantNode>(scalarName);
        auto scalarFunctionNode = std::make_shared<DB::FunctionNode>("__getScalar");
        scalarFunctionNode->getArguments().getNodes().emplace_back(std::move(scalarNode));
        auto tableFunctionNode = std::make_shared<DB::TableFunctionNode>("ytSubquery");
        tableFunctionNode->getArguments().getNodes().emplace_back(std::move(scalarFunctionNode));

        scalars[scalarName] = DB::Block{{DB::DataTypeString().createColumnConst(1, std::string(encodedSpec)), std::make_shared<DB::DataTypeString>(), "scalarName"}};

        tableExpressionNode->setAlias("__" + scalarName);
        if (tableExpressionNode->hasAlias()) {
            tableFunctionNode->setAlias(tableExpressionNode->getAlias());
        }

        newTableExpressions.emplace_back(std::move(tableFunctionNode));
    }

    ReplaceTableExpressions(newTableExpressions);

    const auto& executionSettings = StorageContext_->Settings->Execution;

    bool filterJoinedSubquery = executionSettings->FilterJoinedSubqueryBySortKey || RightOrFullJoin_;

    if (!TwoYTTableJoin_ && filterJoinedSubquery && JoinedByKeyColumns_ && !threadSubqueries.empty()) {
        // TODO(max42): this comparator should be created beforehand.
        TComparator comparator(std::vector<ESortOrder>(KeyColumnCount_, ESortOrder::Ascending));

        TOwningKeyBound lowerBound;
        TOwningKeyBound upperBound;

        if (RightOrFullJoin_) {
            // For right or full join we need to distribute all rows
            // even if they do not match to anything in left table.
            if (PreviousUpperBound_) {
                lowerBound = PreviousUpperBound_.Invert();
            }
            if (!isLastSubquery) {
                upperBound = threadSubqueries.Back().Bounds.second;
            }
            PreviousUpperBound_ = upperBound;
        } else {
            lowerBound = threadSubqueries.Front().Bounds.first;
            upperBound = threadSubqueries.Back().Bounds.second;
        }

        if (lowerBound && upperBound) {
            YT_VERIFY(comparator.CompareKeyBounds(lowerBound, upperBound) <= 0);
        }
        AddBoundConditionToJoinedSubquery(lowerBound, upperBound);
    }

    auto secondaryQueryAst = DB::queryNodeToSelectQuery(QueryInfo_.query_tree);

    RollbackModifications();

    YT_LOG_TRACE("Restoring qualified names (QueryBefore: %v)", *secondaryQueryAst);

    DB::RestoreQualifiedNamesVisitor::Data data;
    DB::RestoreQualifiedNamesVisitor(data).visit(secondaryQueryAst);

    YT_LOG_DEBUG("Query rewritten (NewQuery: %v)", *secondaryQueryAst);

    return {std::move(secondaryQueryAst), std::move(scalars)};
}

IStorageDistributorPtr TQueryAnalyzer::GetStorage(const DB::QueryTreeNodePtr& tableExpression) const
{
    if (!tableExpression) {
        return nullptr;
    }

    DB::StoragePtr storage;
    auto tableExpressionNodeType = tableExpression->getNodeType();
    if (tableExpressionNodeType == DB::QueryTreeNodeType::TABLE) {
        storage = tableExpression->as<DB::TableNode&>().getStorage();
    } else if (tableExpressionNodeType == DB::QueryTreeNodeType::TABLE_FUNCTION) {
        storage = tableExpression->as<DB::TableFunctionNode&>().getStorage();
    } else {
        return nullptr;
    }

    return std::dynamic_pointer_cast<IStorageDistributor>(storage);
}

void TQueryAnalyzer::ApplyModification(DB::QueryTreeNodePtr* queryPart, DB::QueryTreeNodePtr newValue, DB::QueryTreeNodePtr previousValue)
{
    YT_LOG_DEBUG("Replacing query part (QueryPart: %v, NewValue: %v)", previousValue->toAST(), newValue->toAST());
    Modifications_.emplace_back(queryPart, std::move(previousValue));
    *queryPart = std::move(newValue);
}

void TQueryAnalyzer::ApplyModification(DB::QueryTreeNodePtr* queryPart, DB::QueryTreeNodePtr newValue)
{
    ApplyModification(queryPart, newValue, *queryPart);
}

void TQueryAnalyzer::RollbackModifications()
{
    YT_LOG_DEBUG("Rolling back modifications (ModificationCount: %v)", Modifications_.size());
    while (!Modifications_.empty()) {
        auto& [queryPart, oldValue] = Modifications_.back();
        *queryPart = std::move(oldValue);
        Modifications_.pop_back();
    }
}

void TQueryAnalyzer::AddBoundConditionToJoinedSubquery(
    TOwningKeyBound lowerBound,
    TOwningKeyBound upperBound)
{
    YT_LOG_DEBUG("Adding bound condition to joined subquery (LowerLimit: %v, UpperLimit: %v)",
        lowerBound,
        upperBound);

    auto createBoundCondition = [&] (const auto& bound) -> DB::ASTPtr {
        if (bound.IsUniversal()) {
            return std::make_shared<DB::ASTLiteral>(true);
        }
        if (bound.IsEmpty()) {
            return std::make_shared<DB::ASTLiteral>(false);
        }

        auto tableSchema = Storages_[0]->GetSchema();
        auto boundLiterals = UnversionedRowToFields(bound.Prefix, *tableSchema);
        // The bound is not universal nor empty, so it should contain at least one literal.
        YT_VERIFY(!boundLiterals.empty());

        int literalsSize = boundLiterals.size();

        std::vector<DB::ASTPtr> joinKeyExpressions;
        joinKeyExpressions.reserve(JoinKeyRightExpressions_.size());
        std::transform(
            JoinKeyRightExpressions_.begin(),
            JoinKeyRightExpressions_.end(),
            std::back_inserter(joinKeyExpressions),
            [](const DB::QueryTreeNodePtr& expression) { return expression->toAST(); });

        if (literalsSize < KeyColumnCount_) {
            joinKeyExpressions.resize(literalsSize);
        }

        bool keepNulls = StorageContext_->Settings->Execution->KeepNullsInRightOrFullJoin;

        auto condition = CreateKeyComparison(
            (bound.IsUpper ? "less" : "greater"),
            /*isOrEquals*/ bound.IsInclusive,
            std::move(joinKeyExpressions),
            std::move(boundLiterals),
            /*careAboutNulls*/ RightOrFullJoin_ && keepNulls);

        return condition;
    };

    DB::ASTs conjunctionArgs = {};

    if (lowerBound) {
        YT_VERIFY(!lowerBound.IsUpper);
        conjunctionArgs.emplace_back(createBoundCondition(lowerBound));
    }

    if (upperBound) {
        YT_VERIFY(upperBound.IsUpper);
        conjunctionArgs.emplace_back(createBoundCondition(upperBound));
    }

    YT_VERIFY(std::ssize(JoinKeyRightExpressions_) == KeyColumnCount_);

    if (conjunctionArgs.empty()) {
        return;
    }

    DB::ASTPtr boundConditions = DB::makeASTForLogicalAnd(std::move(conjunctionArgs));

    YT_LOG_TRACE("Bound conditions generated (LowerBound: %v, UpperBound: %v, Conditions: %v)",
        lowerBound,
        upperBound,
        *boundConditions);

    YT_VERIFY(TableExpressions_.size() == 2 && TableExpressions_[1]);
    YT_VERIFY(TableExpressionPtrs_.size() == 2 && TableExpressionPtrs_[1]);

    // Adding where condition into existing table expression is difficult or impossible because of:
    // 1. Table expression is a table function (e.g. numberes(10)) or not-yt table identifier (e.g. system.clique).
    // 2. Table expression is a subquery with complex structure with unions (e.g. ((select 1) union (select 1 union select 2)) ).
    // 3. Key column names could be qualified with subquery alias (e.g. (...) as a join (...) as b on a.x = b.y).
    //    This alias is not accessible inside the subquery.
    // The simplest way to add conditions in such expressions is to wrap them with 'select * from <table expression>'.
    auto newTableExpressionNode = std::make_shared<DB::QueryNode>(DB::Context::createCopy(getContext()));

    auto selectListNode = std::make_shared<DB::ListNode>();
    selectListNode->getNodes().push_back(std::make_shared<DB::MatcherNode>());
    newTableExpressionNode->getProjectionNode() = std::move(selectListNode);
    newTableExpressionNode->getWhere() = DB::buildQueryTree(std::move(boundConditions), getContext());
    newTableExpressionNode->getJoinTree() = TableExpressions_[1]->clone();
    newTableExpressionNode->setIsSubquery(true);
    newTableExpressionNode->setAlias(TableExpressions_[1]->getAlias());

    // Replace the whole table expression.
    ApplyModification(TableExpressionPtrs_[1], std::move(newTableExpressionNode));
}

void TQueryAnalyzer::ReplaceTableExpressions(std::vector<DB::QueryTreeNodePtr> newTableExpressions)
{
    YT_VERIFY(std::ssize(newTableExpressions) == YtTableCount_);
    for (int index = 0; index < std::ssize(newTableExpressions); ++index) {
        YT_VERIFY(newTableExpressions[index]);
        ApplyModification(TableExpressionPtrs_[index], newTableExpressions[index]);
    }
}

bool TQueryAnalyzer::HasJoinWithTwoTables() const
{
    return TwoYTTableJoin_;
}

bool TQueryAnalyzer::HasGlobalJoin() const
{
    return GlobalJoin_;
}

bool TQueryAnalyzer::HasInOperator() const
{
    return HasInOperator_;
}

void TQueryAnalyzer::Prepare()
{
    if (Prepared_) {
        return;
    }

    const auto& settings = StorageContext_->Settings;

    bool needSortedPool = (TwoYTTableJoin_ && !CrossJoin_) || RightOrFullJoin_;
    bool filterJoinedTableBySortedKey = settings->Execution->FilterJoinedSubqueryBySortKey && Join_ && !CrossJoin_;

    if (needSortedPool || filterJoinedTableBySortedKey) {
        InferSortedJoinKeyColumns(needSortedPool);
    }
    if (settings->Execution->OptimizeQueryProcessingStage) {
        OptimizeQueryProcessingStage();
    }
    if (settings->Execution->EnableOptimizeReadInOrder) {
        InferReadInOrderMode(settings->Execution->AssumeNoNullKeys, settings->Execution->AssumeNoNanKeys);
    }

    Prepared_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
