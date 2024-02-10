#include "query_analyzer.h"

#include "computed_columns.h"
#include "config.h"
#include "format.h"
#include "helpers.h"
#include "helpers.h"
#include "host.h"
#include "query_context.h"
#include "query_context.h"
#include "std_helpers.h"
#include "subquery.h"
#include "table.h"

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

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
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/makeASTForLogicalFunction.h>

#include <library/cpp/string_utils/base64/base64.h>

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

//! Find the longest prefix of key columns from the schema used in keyAst.
int GetUsedKeyPrefixSize(const DB::ASTPtr& keyAst, const TTableSchemaPtr& schema)
{
    if (!schema->IsSorted()) {
        return 0;
    }

    THashSet<TString> usedColumns;

    for (int index = 0; index < std::ssize(keyAst->children); ++index) {
        const auto& element = keyAst->children[index];
        const auto* identifier = element->as<DB::ASTIdentifier>();
        if (identifier) {
            usedColumns.emplace(identifier->shortName());
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
    ParseQuery();
}

void TQueryAnalyzer::InferSortedJoinKeyColumns(bool needSortedPool)
{
    YT_VERIFY(Join_ && !CrossJoin_ && Storages_.size() == 2);

    const auto& leftStorage = Storages_[0];
    const auto& rightStorage = Storages_[1];

    const auto& leftTableSchema = *leftStorage->GetSchema();
    const auto& rightTableSchema = (rightStorage ? *rightStorage->GetSchema() : TTableSchema());

    const auto& analyzedJoin = QueryInfo_.syntax_analyzer_result->analyzed_join;

    auto joinLeftKeysAst = analyzedJoin->leftKeysList();
    auto joinRightKeysAst = (analyzedJoin->hasOn() ? analyzedJoin->rightKeysList() : joinLeftKeysAst);
    const auto& joinLeftKeys = joinLeftKeysAst->children;
    const auto& joinRightKeys = joinRightKeysAst->children;

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
                << TErrorAttribute("table_expression", tableExpression)
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
    std::vector<DB::ASTPtr> joinKeyExpressions(leftTableSchema.GetKeyColumnCount());

    if (TwoYTTableJoin_) {
        // Two YT table join always requires sorted pool.
        YT_VERIFY(needSortedPool);

        // Both tables should be sorted and simple (concatYtTables is forbidden).
        checkTableSorted(/*tableIndex*/ 0);
        checkTableSorted(/*tableIndex*/ 1);

        for (int index = 0; index < joinKeySize; ++index) {
            const auto* leftKeyColumn = joinLeftKeys[index]->as<DB::ASTIdentifier>();
            const auto* rightKeyColumn = joinRightKeys[index]->as<DB::ASTIdentifier>();

            // Cannot match expressions.
            if (!leftKeyColumn || !rightKeyColumn) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index], joinRightKeys[index]);
                continue;
            }

            int leftKeyPosition = getKeyColumnPosition(leftKeyPositionMap, TString(leftKeyColumn->shortName()));
            int rightKeyPosition = getKeyColumnPosition(rightKeyPositionMap, TString(rightKeyColumn->shortName()));

            // Cannot match keys in different positions.
            if (leftKeyPosition == -1 || rightKeyPosition == -1 || leftKeyPosition != rightKeyPosition) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index], joinRightKeys[index]);
                continue;
            }

            matchedLeftKeyNames.emplace(leftKeyColumn->shortName());
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
            const auto* leftKeyColumn = joinLeftKeys[index]->as<DB::ASTIdentifier>();
            // TODO(dakovalkov): Check that expression is deterministic.
            const auto& rightKeyExpression = joinRightKeys[index];

            if (!leftKeyColumn) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index], joinRightKeys[index]);
                continue;
            }

            int keyPosition = getKeyColumnPosition(leftKeyPositionMap, TString(leftKeyColumn->shortName()));
            // Not a key column, ignore it.
            if (keyPosition == -1) {
                unmatchedKeyPairs.emplace_back(joinLeftKeys[index], joinRightKeys[index]);
                continue;
            }

            matchedLeftKeyNames.emplace(leftKeyColumn->shortName());

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

struct TInOperatorMatcher
{
    struct Data
    {
        bool HasInOperator = false;
    };

    static bool needChildVisit(const DB::ASTPtr& node, const DB::ASTPtr& child)
    {
        if (const auto* select = node->as<DB::ASTSelectQuery>()) {
            if (child == select->having()) {
                return false;
            }
        }

        return !child->as<DB::ASTSubquery>();
    }

    static void visit(DB::ASTPtr& ast, Data& data)
    {
        if (const auto* functionAst = ast->as<DB::ASTFunction>()) {
            if (DB::functionIsInOrGlobalInOperator(functionAst->name)) {
                if (functionAst->arguments->children.size() != 2) {
                    THROW_ERROR_EXCEPTION("Wrong number of arguments passed to function (FunctionName: %v, NumberOfArguments: %v)",
                        functionAst->name,
                        functionAst->arguments->children.size());
                }

                auto rhs = functionAst->arguments->children[1];
                if (rhs->as<DB::ASTSubquery>() || rhs->as<DB::ASTTableIdentifier>()) {
                    data.HasInOperator = true;
                    return;
                }
            }
        }
    }
};

using TInOperatorVisitor = DB::InDepthNodeVisitor<TInOperatorMatcher, true>;

void TQueryAnalyzer::ParseQuery()
{
    auto* selectQuery = QueryInfo_.query->as<DB::ASTSelectQuery>();

    YT_LOG_DEBUG("Analyzing query (Query: %v)", *selectQuery);

    YT_VERIFY(selectQuery);
    YT_VERIFY(selectQuery->tables());

    auto* tablesInSelectQuery = selectQuery->tables()->as<DB::ASTTablesInSelectQuery>();
    YT_VERIFY(tablesInSelectQuery);
    YT_VERIFY(tablesInSelectQuery->children.size() >= 1);
    // There may be maximum 3 children: the main table, 1 joined table and 1 array join.
    YT_VERIFY(tablesInSelectQuery->children.size() <= 3);

    TInOperatorMatcher::Data data;
    TInOperatorVisitor(data).visit(QueryInfo_.query);
    HasInOperator_ = data.HasInOperator;

    for (int index = 0; index < std::ssize(tablesInSelectQuery->children); ++index) {
        auto& tableInSelectQuery = tablesInSelectQuery->children[index];
        auto* tablesElement = tableInSelectQuery->as<DB::ASTTablesInSelectQueryElement>();
        YT_VERIFY(tablesElement);

        if (tablesElement->array_join) {
            // First element should always be a table expression.
            YT_VERIFY(index != 0);
            continue;
        }
        YT_VERIFY(tablesElement->table_expression);

        YT_LOG_DEBUG("Found table expression (Index: %v, TableExpression: %v)", index, *tablesElement->table_expression);

        if (index != 0) {
            Join_ = true;
            YT_VERIFY(tablesElement->table_join);
            const auto* tableJoin = tablesElement->table_join->as<DB::ASTTableJoin>();
            YT_VERIFY(tableJoin);
            if (static_cast<int>(tableJoin->locality) == static_cast<int>(DB::JoinLocality::Global)) {
                YT_LOG_DEBUG("Table expression is a global join (Index: %v)", index);
                GlobalJoin_ = true;
            }
            if (static_cast<int>(tableJoin->kind) == static_cast<int>(DB::JoinKind::Right) ||
                static_cast<int>(tableJoin->kind) == static_cast<int>(DB::JoinKind::Full))
            {
                YT_LOG_DEBUG("Query is a right or full join");
                RightOrFullJoin_ = true;
            }
            if (static_cast<int>(tableJoin->kind) == static_cast<int>(DB::JoinKind::Cross)) {
                YT_LOG_DEBUG("Query is a cross join");
                CrossJoin_ = true;
            }
        } else {
            YT_VERIFY(!tablesElement->table_join);
        }

        auto& tableExpression = tablesElement->table_expression;
        TableExpressions_.emplace_back(tableExpression->as<DB::ASTTableExpression>());
        YT_VERIFY(TableExpressions_.back());
        TableExpressionPtrs_.emplace_back(&tableExpression);
    }

    // At least first table expression should be the one that instantiated this query analyzer (aka owner).
    YT_VERIFY(TableExpressions_.size() >= 1);
    // More than 2 tables are not supported in CH yet.
    YT_VERIFY(TableExpressions_.size() <= 2);

    for (size_t tableExpressionIndex = 0; tableExpressionIndex < TableExpressions_.size(); ++tableExpressionIndex) {
        const auto& tableExpression = TableExpressions_[tableExpressionIndex];

        auto& storage = Storages_.emplace_back(GetStorage(tableExpression));
        if (storage) {
            YT_LOG_DEBUG("Table expression corresponds to TStorageDistributor (TableExpression: %v)",
                *tableExpression);
            ++YtTableCount_;
        } else {
            YT_LOG_DEBUG("Table expression does not correspond to TStorageDistributor (TableExpression: %v)",
                *tableExpression);
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

void TQueryAnalyzer::OptimizeQueryProcessingStage()
{
    const auto& settings = StorageContext_->Settings;

    const auto& select = QueryInfo_.query->as<DB::ASTSelectQuery&>();
    const auto& storage = Storages_[0];

    bool allowPoolSwitch = settings->Execution->AllowSwitchToSortedPool;
    bool allowKeyCut = settings->Execution->AllowKeyTruncating;

    bool extremes = getContext()->getSettingsRef().extremes;

    // We can always execute query up to WithMergeableState.
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::WithMergeableState;

    // Some checks from native CH routine.
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube) {
        return;
    }
    if (QueryInfo_.has_window) {
        return;
    }
    if (extremes) {
        return;
    }

    bool isSingleTable = (storage->GetTables().size() == 1);
    int keyColumnCount = KeyColumnCount_;

    auto processAggregationKeyAst = [&] (const DB::ASTPtr& keyAst) {
        // SortedPool expects non-overlapping sorted chunks. In case of multiple
        // tables (concatYtTablesRange), this condition is broken,
        // so we cannot use SortedPool to optimize aggregation.
        // TODO(dakovalkov): Theoretically, the optimization is still possible,
        // but we need to overthink SortedPool logic.
        if (!isSingleTable) {
            return false;
        }

        int usedKeyColumnCount = GetUsedKeyPrefixSize(keyAst, storage->GetSchema());

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

    // Simple aggregation without groupBy, e.g. 'select avg(x) from table'.
    if (!QueryInfo_.syntax_analyzer_result->aggregates.empty() && !select.groupBy()) {
        return;
    }
    if (select.groupBy() && !processAggregationKeyAst(select.groupBy())) {
        return;
    }
    if (select.distinct && !processAggregationKeyAst(select.select())) {
        return;
    }
    if (select.limitBy() && !processAggregationKeyAst(select.limitBy())) {
        return;
    }

    // All aggregations can be performed on instances, switching to 'AfterAggregation' stage.
    YT_VERIFY(KeyColumnCount_ == 0 || keyColumnCount <= KeyColumnCount_);
    KeyColumnCount_ = keyColumnCount;
    if (!JoinKeyRightExpressions_.empty()) {
        JoinKeyRightExpressions_.resize(KeyColumnCount_);
    }
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    if (select.limitLength() || select.limitOffset()) {
        return;
    }
    if (select.orderBy()) {
        // TODO(dakovalkov): We can also analyze orderBy AST.
        // This information won't help to optimize query processing stage,
        // but it could be used to perform distributed insert into sorted table.
        return;
    }

    // Query does not contain Limit/OrderBy clauses, the result is complete.
    OptimizedQueryProcessingStage_ = DB::QueryProcessingStage::Complete;
}

TQueryAnalysisResult TQueryAnalyzer::Analyze() const
{
    if (!Prepared_) {
        THROW_ERROR_EXCEPTION("Query analyzer is not prepared but Analyze method is already called; "
            "this is a bug; please, file an issue in CHYT queue");
    }

    const auto& settings = StorageContext_->Settings;

    TQueryAnalysisResult result;

    for (const auto& storage : Storages_) {
        if (!storage) {
            continue;
        }
        result.Tables.emplace_back(storage->GetTables());
        auto schema = storage->GetSchema();
        std::optional<DB::KeyCondition> keyCondition;
        if (schema->IsSorted()) {
            auto primaryKeyExpression = std::make_shared<DB::ExpressionActions>(std::make_shared<DB::ActionsDAG>(
                ToNamesAndTypesList(*schema, settings->Composite)));

            auto queryInfoForKeyCondition = QueryInfo_;

            if (settings->EnableComputedColumnDeduction) {
                // Query may not contain deducible values for computed columns.
                // We populate query with deducible equations on computed columns,
                // so key condition is able to properly filter ranges with computed
                // key columns.
                queryInfoForKeyCondition.query = queryInfoForKeyCondition.query->clone();
                auto* selectQuery = queryInfoForKeyCondition.query->as<DB::ASTSelectQuery>();
                YT_VERIFY(selectQuery);

                if (selectQuery->where()) {
                    selectQuery->refWhere() = PopulatePredicateWithComputedColumns(
                        selectQuery->where(),
                        schema,
                        getContext(),
                        *queryInfoForKeyCondition.prepared_sets,
                        settings,
                        Logger);
                }
            }

            keyCondition.emplace(queryInfoForKeyCondition, getContext(), ToNames(schema->GetKeyColumns()), primaryKeyExpression);
        }
        result.KeyConditions.emplace_back(std::move(keyCondition));
        result.TableSchemas.emplace_back(storage->GetSchema());
    }

    result.PoolKind = (KeyColumnCount_ > 0 ? EPoolKind::Sorted : EPoolKind::Unordered);
    result.KeyColumnCount = KeyColumnCount_;

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

    std::vector<DB::ASTPtr> newTableExpressions;

    DB::Scalars scalars;

    for (int index = 0; index < YtTableCount_; ++index) {
        auto tableExpression = TableExpressions_[index];

        std::vector<TChunkStripePtr> stripes;
        for (const auto& subquery : threadSubqueries) {
            stripes.emplace_back(subquery.StripeList->Stripes[index]);
        }

        auto spec = specTemplate;
        spec.TableIndex = index;
        spec.ReadSchema = Storages_[index]->GetSchema();

        FillDataSliceDescriptors(spec, miscExtMap, MakeRange(stripes));

        auto protoSpec = NYT::ToProto<NProto::TSubquerySpec>(spec);
        auto encodedSpec = protoSpec.SerializeAsString();

        YT_LOG_DEBUG("Serializing subquery spec (TableIndex: %v, SpecLength: %v)", index, encodedSpec.size());

        std::string scalarName = "yt_table_" + std::to_string(index);
        auto scalar = DB::makeASTFunction("__getScalar", std::make_shared<DB::ASTLiteral>(scalarName));
        auto tableFunction = DB::makeASTFunction("ytSubquery", std::move(scalar));

        scalars[scalarName] = DB::Block{{DB::DataTypeString().createColumnConst(1, std::string(encodedSpec)), std::make_shared<DB::DataTypeString>(), "scalarName"}};

        if (tableExpression->database_and_table_name) {
            DB::DatabaseAndTableWithAlias databaseAndTable(tableExpression->database_and_table_name);
            if (!databaseAndTable.alias.empty()) {
                tableFunction->alias = databaseAndTable.alias;
            } else {
                tableFunction->alias = databaseAndTable.table;
            }
        } else {
            tableFunction->alias = static_cast<DB::ASTWithAlias&>(*tableExpression->table_function).alias;
        }

        auto clonedTableExpression = tableExpression->clone();
        auto* typedTableExpression = clonedTableExpression->as<DB::ASTTableExpression>();
        YT_VERIFY(typedTableExpression);
        typedTableExpression->table_function = std::move(tableFunction);
        typedTableExpression->database_and_table_name = nullptr;
        typedTableExpression->subquery = nullptr;
        typedTableExpression->sample_offset = nullptr;
        typedTableExpression->sample_size = nullptr;

        newTableExpressions.emplace_back(std::move(clonedTableExpression));
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

    auto secondaryQueryAst = QueryInfo_.query->clone();

    RollbackModifications();

    YT_LOG_TRACE("Restoring qualified names (QueryBefore: %v)", *secondaryQueryAst);

    DB::RestoreQualifiedNamesVisitor::Data data;
    DB::RestoreQualifiedNamesVisitor(data).visit(secondaryQueryAst);

    YT_LOG_DEBUG("Query rewritten (NewQuery: %v)", *secondaryQueryAst);

    return {std::move(secondaryQueryAst), std::move(scalars)};
}

IStorageDistributorPtr TQueryAnalyzer::GetStorage(const DB::ASTTableExpression* tableExpression) const
{
    if (!tableExpression) {
        return nullptr;
    }
    DB::StoragePtr storage;
    if (tableExpression->table_function) {
        storage = getContext()->getQueryContext()->executeTableFunction(tableExpression->table_function);
    } else if (tableExpression->database_and_table_name) {
        DB::StorageID storageId(tableExpression->database_and_table_name);
        // Resolve database name if it's not specified explicitly.
        storageId = getContext()->resolveStorageID(std::move(storageId));
        if (storageId.database_name == "YT") {
            storage = DB::DatabaseCatalog::instance().getTable(storageId, getContext());
        }
    }

    return std::dynamic_pointer_cast<IStorageDistributor>(storage);
}

void TQueryAnalyzer::ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue, DB::ASTPtr previousValue)
{
    YT_LOG_DEBUG("Replacing query part (QueryPart: %v, NewValue: %v)", previousValue, newValue);
    Modifications_.emplace_back(queryPart, std::move(previousValue));
    *queryPart = std::move(newValue);
}

void TQueryAnalyzer::ApplyModification(DB::ASTPtr* queryPart, DB::ASTPtr newValue)
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

        auto joinKeyExpressions = JoinKeyRightExpressions_;
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
    auto newTableExpression = WrapTableExpressionWithSubquery(
        *TableExpressionPtrs_[1],
        /*columnNames*/ std::nullopt,
        std::move(boundConditions));

    // Replace the whole table expression.
    ApplyModification(TableExpressionPtrs_[1], std::move(newTableExpression));
}

void TQueryAnalyzer::ReplaceTableExpressions(std::vector<DB::ASTPtr> newTableExpressions)
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

    Prepared_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
