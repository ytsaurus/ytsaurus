#include "query_preparer.h"

#include "ast_visitors.h"
#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "lexer.h"
#include "push_down_group_by.h"
#include "query_helpers.h"
#include "query_visitors.h"
#include "expr_builder_base.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/misc/variant.h>

#include <unordered_set>

namespace NYT::NQueryClient {

using namespace NCodegen;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxQueryLimit = std::numeric_limits<i64>::max() - 2;
constexpr int MaxJoinNumber = 200;
constexpr int MaxMultiJoinGroupNumber = 15;

EValueType ComparableTypes[] = {
    EValueType::Boolean,
    EValueType::Int64,
    EValueType::Uint64,
    EValueType::Double,
    EValueType::String,
    EValueType::Any,
    EValueType::Composite,
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<std::string>* functions);

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& exprs,
    std::vector<std::string>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<std::string>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(ToLower(functionExpr->FunctionName));
        ExtractFunctionNames(functionExpr->Arguments, functions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        ExtractFunctionNames(unaryExpr->Operand, functions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        ExtractFunctionNames(binaryExpr->Lhs, functions);
        ExtractFunctionNames(binaryExpr->Rhs, functions);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        ExtractFunctionNames(inExpr->Expr, functions);
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        ExtractFunctionNames(betweenExpr->Expr, functions);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        ExtractFunctionNames(transformExpr->Expr, functions);
        ExtractFunctionNames(transformExpr->DefaultExpr, functions);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        ExtractFunctionNames(caseExpr->OptionalOperand, functions);
        ExtractFunctionNames(caseExpr->WhenThenExpressions, functions);
        ExtractFunctionNames(caseExpr->DefaultExpression, functions);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        ExtractFunctionNames(likeExpr->Text, functions);
        ExtractFunctionNames(likeExpr->Pattern, functions);
        ExtractFunctionNames(likeExpr->EscapeCharacter, functions);
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else if (auto aliasExpr = expr->As<NAst::TAliasExpression>()) {
        ExtractFunctionNames(aliasExpr->Expression, functions);
    } else if (auto queryExpr = expr->As<NAst::TQueryExpression>()) {
        ExtractFunctionNames(queryExpr->Query.WherePredicate, functions);
        ExtractFunctionNames(queryExpr->Query.SelectExprs, functions);
    } else {
        YT_ABORT();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<std::string>* functions)
{
    if (!exprs) {
        return;
    }

    CheckStackDepth();

    for (const auto& expr : *exprs) {
        ExtractFunctionNames(expr, functions);
    }
}

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& whenThenExpressions,
    std::vector<std::string>* functions)
{
    CheckStackDepth();

    for (const auto& [condition, result] : whenThenExpressions) {
        ExtractFunctionNames(condition, functions);
        ExtractFunctionNames(result, functions);
    }
}

std::vector<std::string> ExtractFunctionNames(
    const NAst::TQuery& query,
    const NAst::TAliasMap& aliasMap)
{
    std::vector<std::string> functions;

    ExtractFunctionNames(query.WherePredicate, &functions);
    ExtractFunctionNames(query.HavingPredicate, &functions);
    ExtractFunctionNames(query.SelectExprs, &functions);

    if (query.GroupExprs) {
        for (const auto& expr : *query.GroupExprs) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& join : query.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                ExtractFunctionNames(tableJoin.Lhs, &functions);
                ExtractFunctionNames(tableJoin.Rhs, &functions);
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                ExtractFunctionNames(arrayJoin.Columns, &functions);
            });
    }

    for (const auto& orderExpression : query.OrderExpressions) {
        for (const auto& expr : orderExpression.Expressions) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& aliasedExpression : aliasMap) {
        ExtractFunctionNames(aliasedExpression.second, &functions);
    }

    Visit(query.FromClause,
        [&] (const NAst::TTableDescriptor& /*table*/) { },
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            auto extracted = ExtractFunctionNames(subquery->Ast, aliasMap);
            functions.insert(
                functions.end(),
                std::make_move_iterator(extracted.begin()),
                std::make_move_iterator(extracted.end()));
        },
        [&] (const NAst::TExpressionList& expressions) {
            ExtractFunctionNames(expressions, &functions);
        });


    std::sort(functions.begin(), functions.end());
    functions.erase(
        std::unique(functions.begin(), functions.end()),
        functions.end());

    return functions;
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr BuildPredicate(
    const NAst::TExpressionList& expressionAst,
    TExpressionBuilder* builder,
    TStringBuf name)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            << TErrorAttribute("source", FormatExpression(expressionAst));
    }

    // TODO(lukyan): BuildTypedExpression(expressionAst.front(), {EValueType::Boolean}) ?
    auto typedPredicate = builder->BuildTypedExpression(expressionAst.front());

    auto actualType = typedPredicate->GetWireType();
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("%v is not a boolean expression", name)
            << TErrorAttribute("source", expressionAst.front()->GetSource(builder->GetSource()))
            << TErrorAttribute("actual_type", actualType)
            << TErrorAttribute("expected_type", expectedType);
    }

    return ApplyRewriters(typedPredicate);
}

TGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    TExpressionBuilder* builder)
{
    auto groupClause = New<TGroupClause>();
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder->BuildTypedExpression(expressionAst, ComparableTypes);
        groupClause->AddGroupItem(ApplyRewriters(typedExpr), builder->InferGroupItemName(typedExpr, *expressionAst));
    }

    builder->SetGroupData(
        &groupClause->GroupItems,
        &groupClause->AggregateItems);

    return groupClause;
}

void DropLimitClauseWhenGroupByOne(const TQueryPtr& query)
{
    if (!query->OrderClause &&
        query->GroupClause &&
        std::ssize(query->GroupClause->GroupItems) == 1 &&
        query->GroupClause->GroupItems[0].Expression->As<TLiteralExpression>() &&
        query->Limit != UnorderedReadHint &&
        query->Limit != 0)
    {
        query->Limit = UnorderedReadHint;
    }
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    TExpressionBuilder* builder)
{
    if (ast.WherePredicate) {
        auto wherePredicate = BuildPredicate(*ast.WherePredicate, builder, "WHERE-clause");
        query->WhereClause = IsTrue(wherePredicate) ? nullptr : wherePredicate;
    }

    if (ast.GroupExprs) {
        auto groupClause = BuildGroupClause(*ast.GroupExprs, ast.TotalsMode, builder);

        auto keyColumns = query->GetKeyColumns();

        TNamedItemList groupItems = std::move(groupClause->GroupItems);

        std::vector<int> touchedKeyColumns(keyColumns.size(), -1);
        for (int index = 0; index < std::ssize(groupItems); ++index) {
            const auto& item = groupItems[index];
            if (auto referenceExpr = item.Expression->As<TReferenceExpression>()) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    touchedKeyColumns[keyPartIndex] = index;
                }
            }
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < touchedKeyColumns.size(); ++keyPrefix) {
            if (touchedKeyColumns[keyPrefix] >= 0) {
                continue;
            }

            const auto& expression = query->Schema.Original->Columns()[keyPrefix].Expression();

            if (!expression) {
                break;
            }

            // Call PrepareExpression to extract references only.
            THashSet<std::string> references;
            PrepareExpression(*expression, *query->Schema.Original, builder->Functions(), &references);

            auto canEvaluate = true;
            for (const auto& reference : references) {
                int referenceIndex = query->Schema.Original->GetColumnIndexOrThrow(reference);
                if (touchedKeyColumns[referenceIndex] < 0) {
                    canEvaluate = false;
                }
            }

            if (!canEvaluate) {
                break;
            }
        }

        touchedKeyColumns.resize(keyPrefix);
        for (int index : touchedKeyColumns) {
            if (index >= 0) {
                groupClause->GroupItems.push_back(std::move(groupItems[index]));
            }
        }

        groupClause->CommonPrefixWithPrimaryKey = groupClause->GroupItems.size();

        for (auto& item : groupItems) {
            if (item.Expression) {
                groupClause->GroupItems.push_back(std::move(item));
            }
        }

        query->GroupClause = groupClause;

        // not prefix, because of equal prefixes near borders
        bool containsPrimaryKey = keyPrefix == query->GetKeyColumns().size();
        // COMPAT(lukyan)
        query->UseDisjointGroupBy = containsPrimaryKey && !keyColumns.empty();
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildPredicate(
            *ast.HavingPredicate,
            builder,
            "HAVING-clause");
    }

    if (!ast.OrderExpressions.empty()) {
        auto orderClause = New<TOrderClause>();

        for (const auto& orderExpr : ast.OrderExpressions) {
            for (const auto& expressionAst : orderExpr.Expressions) {
                auto typedExpr = builder->BuildTypedExpression(
                    expressionAst,
                    ComparableTypes);

                orderClause->OrderItems.push_back({ApplyRewriters(typedExpr), orderExpr.Descending});
            }
        }

        bool canOmitOrderBy = CanOmitOrderBy(0, orderClause->OrderItems, query->GetKeyColumns());
        if (!canOmitOrderBy) {
            query->OrderClause = std::move(orderClause);
        }

        // Use ordered scan otherwise
    }

    if (ast.SelectExprs) {
        auto projectClause = New<TProjectClause>();
        for (const auto& expressionAst : *ast.SelectExprs) {
            auto typedExpr = builder->BuildTypedExpression(expressionAst);

            projectClause->AddProjection(ApplyRewriters(typedExpr), InferColumnName(*expressionAst));
        }

        query->ProjectClause = projectClause;
    } else {
        // Select all columns.
        builder->PopulateAllColumns();
    }

    DropLimitClauseWhenGroupByOne(query);

    builder->Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TYsonToQueryExpressionConvertVisitor
    : public TYsonConsumerBase
{
public:
    explicit TYsonToQueryExpressionConvertVisitor(TStringBuilder* builder)
        : Builder_(builder)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        Builder_->AppendChar('"');
        Builder_->AppendString(EscapeC(value));
        Builder_->AppendChar('"');
    }

    void OnInt64Scalar(i64 value) override
    {
        Builder_->AppendFormat("%v", value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        Builder_->AppendFormat("%vu", value);
    }

    void OnDoubleScalar(double value) override
    {
        Builder_->AppendFormat("%lf", value);
    }

    void OnBooleanScalar(bool value) override
    {
        Builder_->AppendFormat("%lv", value);
    }

    void OnEntity() override
    {
        Builder_->AppendString("null");
    }

    void OnBeginList() override
    {
        Builder_->AppendChar('(');
        InListBeginning_ = true;
    }

    void OnListItem() override
    {
        if (!InListBeginning_) {
            Builder_->AppendString(", ");
        }
        InListBeginning_ = false;
    }

    void OnEndList() override
    {
        Builder_->AppendChar(')');
    }

    void OnBeginMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnKeyedItem(TStringBuf) override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnEndMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

    void OnEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

private:
    TStringBuilder* Builder_;
    bool InListBeginning_;
};

void YsonParseError(TStringBuf message, TYsonStringBuf source)
{
    THROW_ERROR_EXCEPTION("%v", message)
        << TErrorAttribute("context", Format("%v", source.AsStringBuf()));
}

THashMap<std::string, std::string> ConvertYsonPlaceholdersToQueryLiterals(TYsonStringBuf placeholders)
{
    TMemoryInput input{placeholders.AsStringBuf()};
    TYsonPullParser ysonParser{&input, EYsonType::Node};
    TYsonPullParserCursor ysonCursor{&ysonParser};

    if (ysonCursor->GetType() != EYsonItemType::BeginMap) {
        YsonParseError("Incorrect placeholder argument: YSON map expected", placeholders);
    }

    ysonCursor.Next();

    THashMap<std::string, std::string> queryLiterals;
    while (ysonCursor->GetType() != EYsonItemType::EndMap) {
        if (ysonCursor->GetType() != EYsonItemType::StringValue) {
            YsonParseError("Incorrect YSON map placeholder: keys should be strings", placeholders);
        }
        auto key = std::string(ysonCursor->UncheckedAsString());

        ysonCursor.Next();
        switch (ysonCursor->GetType()) {
            case EYsonItemType::EntityValue:
            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::StringValue:
            case EYsonItemType::BeginList: {
                TStringBuilder valueBuilder;
                TYsonToQueryExpressionConvertVisitor ysonValueTransferrer{&valueBuilder};
                ysonCursor.TransferComplexValue(&ysonValueTransferrer);
                queryLiterals.emplace(std::move(key), valueBuilder.Flush());
                break;
            }
            default:
                YsonParseError("Incorrect placeholder map: values should be plain types or lists", placeholders);
        }
    }

    return queryLiterals;
}

NAst::TAstHead ParseQueryString(
    TStringBuf source,
    NAst::TParser::token::yytokentype strayToken,
    TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1)
{
    auto head = NAst::TAstHead();

    THashMap<std::string, std::string> queryLiterals;
    if (placeholderValues) {
        queryLiterals = ConvertYsonPlaceholdersToQueryLiterals(placeholderValues);
    }

    NAst::TLexer lexer(source, strayToken, std::move(queryLiterals), syntaxVersion);
    std::stack<NAst::TAliasMap> aliasMapStack;
    aliasMapStack.push({});
    NAst::TParser parser(lexer, &head, source, std::move(aliasMapStack));

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            << TErrorAttribute("source", source);
    }

    return head;
}

////////////////////////////////////////////////////////////////////////////////

NAst::TParser::token::yytokentype GetStrayToken(EParseMode mode)
{
    switch (mode) {
        case EParseMode::Query:      return NAst::TParser::token::StrayWillParseQuery;
        case EParseMode::JobQuery:   return NAst::TParser::token::StrayWillParseJobQuery;
        case EParseMode::Expression: return NAst::TParser::token::StrayWillParseExpression;
        default:                     YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void EliminateRedundantProjections(const TQueryPtr& innerSubquery, const TTableSchema& outerQueryReadSchema)
{
    auto projectClause = New<TProjectClause>();

    if (!innerSubquery->ProjectClause) {
        for (const auto& column : outerQueryReadSchema.Columns()) {
            projectClause->Projections.push_back(TNamedItem(
                New<TReferenceExpression>(column.LogicalType(), column.Name()),
                column.Name()));
        }
    } else {
        TColumnSet readColumns;
        for (const auto& column : outerQueryReadSchema.Columns()) {
            readColumns.insert(column.Name());
        }

        TNamedItemList filteredProjections;
        for (const auto& projection : innerSubquery->ProjectClause->Projections) {
            if (readColumns.contains(projection.Name)) {
                filteredProjections.push_back(projection);
            }
        }

        projectClause->Projections = std::move(filteredProjections);
    }

    innerSubquery->ProjectClause = std::move(projectClause);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MakeColumnTypesOptional(const TTableSchemaPtr& schema)
{
    std::vector<TColumnSchema> columns = schema->Columns();

    for (auto& column : columns) {
        if (!column.LogicalType()->IsNullable()) {
            column.SetLogicalType(OptionalLogicalType(column.LogicalType()));
        }
    }

    return New<TTableSchema>(columns,
        schema->IsStrict(),
        schema->IsUniqueKeys(),
        schema->GetSchemaModification(),
        schema->DeletedColumns());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(
    TRange<std::string> /*names*/,
    const TTypeInferrerMapPtr& typeInferrers,
    EExecutionBackend /*executionBackend*/)
{
    MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());
}

////////////////////////////////////////////////////////////////////////////////

TParsedSource::TParsedSource(TStringBuf source, NAst::TAstHead astHead)
    : Source(source)
    , AstHead(std::move(astHead))
{ }

std::unique_ptr<TParsedSource> ParseSource(
    TStringBuf source,
    EParseMode mode,
    TYsonStringBuf placeholderValues,
    int syntaxVersion)
{
    return std::make_unique<TParsedSource>(
        source,
        ParseQueryString(
            source,
            GetStrayToken(mode),
            placeholderValues,
            syntaxVersion));
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, int> BuildReferenceToIndexMap(const std::vector<TConstExpressionPtr>& equations)
{
    THashMap<std::string, int> map;

    for (int index = 0; index < std::ssize(equations); ++index) {
        auto& expression = equations[index];
        if (auto* ref = expression->As<TReferenceExpression>()) {
            auto [_, inserted] = map.insert(std::pair(ref->ColumnName, index));
            THROW_ERROR_EXCEPTION_IF(!inserted, "Foreign key column %Qv occurs more than once in a join clause",
                ref->ColumnName);
        }
    }

    return map;
}

std::vector<std::pair<TConstExpressionPtr, int>> MakeExpressionsFromComputedColumns(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& functions,
    const std::optional<std::string>& alias)
{
    std::vector<std::pair<TConstExpressionPtr, int>> expressionsAndColumnIndices;

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        const auto& column = schema->Columns()[index];
        if (!column.Expression()) {
            continue;
        }

        auto evaluatedColumnExpression = PrepareExpression(
            *column.Expression(),
            *schema,
            functions);

        if (alias) {
            TAddAliasRewriter addAliasRewriter{.Alias = alias};
            evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
        }

        expressionsAndColumnIndices.push_back({std::move(evaluatedColumnExpression), index});
    }

    return expressionsAndColumnIndices;
}

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    int expressionBuilderVersion)
{
    return (expressionBuilderVersion == 1
        ? &CreateExpressionBuilderV1
        : &CreateExpressionBuilderV2)(source, functions, aliasMap);
}

TJoinClausePtr BuildJoinClause(
    const TDataSplit& foreignDataSplit,
    const NAst::TJoin& tableJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    size_t* globalCommonKeyPrefix,
    const TTableSchemaPtr& tableSchema,
    const std::optional<std::string>& tableAlias,
    TExpressionBuilder* builder,
    int builderVersion,
    const NLogging::TLogger& Logger)
{
    auto foreignTableSchema = foreignDataSplit.TableSchema;

    auto joinClause = New<TJoinClause>();
    joinClause->Schema.Original = foreignTableSchema;
    joinClause->ForeignObjectId = foreignDataSplit.ObjectId;
    joinClause->IsLeft = tableJoin.IsLeft;

    // BuildPredicate and BuildTypedExpression are used with foreignBuilder.
    auto foreignBuilder = CreateExpressionBuilder(source, functions, aliasMap, builderVersion);

    foreignBuilder->AddTable({
        .Schema = *foreignTableSchema,
        .Alias = tableJoin.Table.Alias,
        .Mapping = &joinClause->Schema.Mapping,
    });

    std::vector<TConstExpressionPtr> selfEquations;
    selfEquations.reserve(tableJoin.Fields.size() + tableJoin.Lhs.size());
    std::vector<TConstExpressionPtr> foreignEquations;
    foreignEquations.reserve(tableJoin.Fields.size() + tableJoin.Rhs.size());

    THashSet<std::string> commonColumnNames;
    // Merge columns.
    for (const auto& referenceExpr : tableJoin.Fields) {
        auto columnName = InferColumnName(referenceExpr->Reference);
        commonColumnNames.insert(columnName);

        auto selfColumnType = builder->ResolveColumn(referenceExpr->Reference);
        auto foreignColumnType = foreignBuilder->ResolveColumn(referenceExpr->Reference);

        if (!selfColumnType || !foreignColumnType) {
            THROW_ERROR_EXCEPTION("Column %Qv not found",
                columnName);
        }

        if (!NTableClient::IsV1Type(selfColumnType) || !NTableClient::IsV1Type(foreignColumnType)) {
            THROW_ERROR_EXCEPTION("Cannot join column %Qv of nonsimple type",
                columnName)
                << TErrorAttribute("self_type", selfColumnType)
                << TErrorAttribute("foreign_type", foreignColumnType);
        }

        // N.B. When we try join optional<int32> and int16 columns it must work.
        if (NTableClient::GetWireType(selfColumnType) != NTableClient::GetWireType(foreignColumnType)) {
            THROW_ERROR_EXCEPTION("Column %Qv type mismatch in join",
                columnName)
                << TErrorAttribute("self_type", selfColumnType)
                << TErrorAttribute("foreign_type", foreignColumnType);
        }

        selfEquations.push_back(New<TReferenceExpression>(selfColumnType, columnName));
        foreignEquations.push_back(New<TReferenceExpression>(foreignColumnType, columnName));
    }

    for (const auto& argument : tableJoin.Lhs) {
        selfEquations.push_back(ApplyRewriters(builder->BuildTypedExpression(argument, ComparableTypes)));
    }
    for (const auto& argument : tableJoin.Rhs) {
        foreignEquations.push_back(ApplyRewriters(foreignBuilder->BuildTypedExpression(argument, ComparableTypes)));
    }

    if (selfEquations.size() != foreignEquations.size()) {
        THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
            selfEquations.size(),
            foreignEquations.size())
            << TErrorAttribute("lhs_source", FormatExpression(tableJoin.Lhs))
            << TErrorAttribute("rhs_source", FormatExpression(tableJoin.Rhs));
    }

    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (selfEquations[index]->GetWireType() != foreignEquations[index]->GetWireType()) {
            THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                InferName(selfEquations[index]),
                InferName(foreignEquations[index]))
                << TErrorAttribute("self_type", selfEquations[index]->LogicalType)
                << TErrorAttribute("foreign_type", foreignEquations[index]->LogicalType);
        }
    }

    // If possible, use ranges, rearrange equations according to foreign key columns, enriching with evaluated columns
    size_t commonKeyPrefix = 0;
    size_t foreignKeyPrefix = 0;
    std::vector<TConstExpressionPtr> keySelfEquations;
    std::vector<TConstExpressionPtr> keyForeignEquations;
    THashSet<int> usedForKeyPrefixEquations;

    auto foreignReferenceToIndexMap = BuildReferenceToIndexMap(foreignEquations);
    auto selfComputedColumnsExpressions = MakeExpressionsFromComputedColumns(tableSchema, functions, tableAlias);

    for (const auto& foreignKeyColumn : foreignTableSchema->Columns()) {
        if (!foreignKeyColumn.SortOrder()) {
            break;
        }

        auto foreignKeyColumnReference = NAst::TReference(foreignKeyColumn.Name(), tableJoin.Table.Alias);
        auto aliasedForeignKeyColumnName = NAst::InferColumnName(foreignKeyColumnReference);
        auto it = foreignReferenceToIndexMap.find(aliasedForeignKeyColumnName);

        if (it != foreignReferenceToIndexMap.end()) {
            keySelfEquations.push_back(selfEquations[it->second]);
            keyForeignEquations.push_back(foreignEquations[it->second]);
            usedForKeyPrefixEquations.insert(it->second);
        } else if (foreignKeyColumn.Expression()) {
            auto evaluatedColumnExpression = PrepareExpression(
                *foreignKeyColumn.Expression(),
                *foreignTableSchema,
                functions);

            if (auto& alias = tableJoin.Table.Alias) {
                TAddAliasRewriter addAliasRewriter{.Alias = *alias};
                evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
            }
            TSelfifyRewriter selfifyRewriter{
                .SelfEquations = selfEquations,
                .ForeignReferenceToIndexMap = foreignReferenceToIndexMap,
            };

            auto matchingSelfExpression = selfifyRewriter.Visit(evaluatedColumnExpression);
            if (!selfifyRewriter.Success) {
                break;
            }

            // The computedSelfEquation might already be among the computed key columns of the self schema.
            // e.g. JOIN equation (A.a, A.b, B.f) = (C.c, C.e, C.q) and table schemas are as follows:
            // A: [hash(a, b), a, b] and C: [hash(c, e), c, d, e]
            // In this case
            for (const auto& [expr, columnIndex] : selfComputedColumnsExpressions) {
                if (Compare(expr, matchingSelfExpression)) {
                    const auto& column = tableSchema->Columns()[columnIndex];
                    auto aliasedReference = NAst::TReference(column.Name(), tableAlias);

                    // Register self evaluated column in the effective schema.
                    builder->ResolveColumn(aliasedReference);

                    matchingSelfExpression = New<TReferenceExpression>(
                        column.LogicalType(),
                        NAst::InferColumnName(aliasedReference));
                    break;
                }
            }

            // Register foreign evaluated column in the effective schema.
            foreignBuilder->ResolveColumn(foreignKeyColumnReference);

            keySelfEquations.push_back(std::move(matchingSelfExpression));
            keyForeignEquations.push_back(New<TReferenceExpression>(
                foreignKeyColumn.LogicalType(),
                aliasedForeignKeyColumnName));
        } else {
            break;
        }

        if (commonKeyPrefix == foreignKeyPrefix &&
            static_cast<int>(commonKeyPrefix) < tableSchema->GetKeyColumnCount())
        {
            if (auto* reference = keySelfEquations.back()->As<TReferenceExpression>()) {
                auto aliasedName = NAst::InferColumnName(NAst::TReference(
                    tableSchema->Columns()[commonKeyPrefix].Name(),
                    tableAlias));
                if (reference->ColumnName == aliasedName) {
                    commonKeyPrefix++;
                }
            }
        }

        foreignKeyPrefix++;
    }

    joinClause->SelfEquations = std::move(keySelfEquations);
    joinClause->ForeignEquations = std::move(keyForeignEquations);
    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (!usedForKeyPrefixEquations.contains(index)) {
            joinClause->SelfEquations.push_back(selfEquations[index]);
            joinClause->ForeignEquations.push_back(foreignEquations[index]);
        }
    }
    *globalCommonKeyPrefix = std::min(*globalCommonKeyPrefix, commonKeyPrefix);
    joinClause->ForeignKeyPrefix = foreignKeyPrefix;
    joinClause->CommonKeyPrefix = *globalCommonKeyPrefix;

    YT_LOG_DEBUG("Creating join (CommonKeyPrefix: %v, ForeignKeyPrefix: %v)",
        joinClause->CommonKeyPrefix,
        joinClause->ForeignKeyPrefix);

    if (tableJoin.Predicate) {
        joinClause->Predicate = BuildPredicate(
            *tableJoin.Predicate,
            foreignBuilder.get(),
            "JOIN-PREDICATE-clause");
    }

    auto schemaAfterJoin = tableJoin.IsLeft ? MakeColumnTypesOptional(foreignTableSchema) : foreignTableSchema;

    builder->AddTable({
        .Schema = *schemaAfterJoin,
        .Alias = tableJoin.Table.Alias,
        .Mapping = &joinClause->Schema.Mapping,
        .SelfJoinedColumns = &joinClause->SelfJoinedColumns,
        .ForeignJoinedColumns = &joinClause->ForeignJoinedColumns,
        .SharedColumns = commonColumnNames,
        .ModifiedSchemaHolder = std::move(schemaAfterJoin),
    });

    return joinClause;
}

TJoinClausePtr BuildArrayJoinClause(
    const NAst::TArrayJoin& arrayJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    TExpressionBuilder* builder,
    int builderVersion)
{
    auto arrayJoinClause = New<TJoinClause>();
    arrayJoinClause->IsLeft = arrayJoin.IsLeft;

    int arrayCount = std::ssize(arrayJoin.Columns);

    TSchemaColumns nestedColumns(arrayCount);
    arrayJoinClause->ArrayExpressions.resize(arrayCount);
    for (int index = 0; index < arrayCount; ++index) {
        const auto& expr = arrayJoin.Columns[index];
        const auto* aliasExpression = expr->As<NAst::TAliasExpression>();
        YT_ASSERT(aliasExpression);

        const auto& typedExpression =
            arrayJoinClause->ArrayExpressions[index] =
                ApplyRewriters(builder->BuildTypedExpression(
                    aliasExpression->Expression,
                    {builderVersion == 1 ? EValueType::Any : EValueType::Composite}));

        auto logicalType = typedExpression->LogicalType;
        auto metatype = logicalType->GetMetatype();
        if (metatype == ELogicalMetatype::Optional) {
            logicalType = logicalType->UncheckedAsOptionalTypeRef().GetElement();
            metatype = logicalType->GetMetatype();
        }

        THROW_ERROR_EXCEPTION_IF(metatype != ELogicalMetatype::List,
            "Expected a list-like type expression in the ARRAY JOIN operator, got %v",
            *typedExpression->LogicalType);

        auto flattenedType = logicalType->UncheckedAsListTypeRef().GetElement();
        if ((arrayJoin.IsLeft || arrayCount > 1) && !flattenedType->IsNullable()) {
            flattenedType = OptionalLogicalType(std::move(flattenedType));
        }
        nestedColumns[index] = TColumnSchema(aliasExpression->Name, std::move(flattenedType));
    }

    arrayJoinClause->Schema.Original = New<TTableSchema>(std::move(nestedColumns));

    ValidateColumnUniqueness(*arrayJoinClause->Schema.Original);

    auto arrayBuilder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap,
        builderVersion);

    arrayBuilder->AddTable({
        .Schema = *arrayJoinClause->Schema.Original,
        .Alias = std::nullopt,
        .Mapping = &arrayJoinClause->Schema.Mapping,
    });

    for (const auto& nestedTableColumn : arrayJoinClause->Schema.Original->Columns()) {
        auto type = arrayBuilder->ResolveColumn(NAst::TReference(nestedTableColumn.Name()));
        YT_ASSERT(type);
    }

    if (arrayJoin.Predicate) {
        arrayJoinClause->Predicate = BuildPredicate(
            *arrayJoin.Predicate,
            arrayBuilder.get(),
            "JOIN-PREDICATE-clause");
    }

    builder->AddTable({
        .Schema = *arrayJoinClause->Schema.Original,
        .Alias = std::nullopt,
        .Mapping = &arrayJoinClause->Schema.Mapping,
        .SelfJoinedColumns = &arrayJoinClause->SelfJoinedColumns,
        .ForeignJoinedColumns = &arrayJoinClause->ForeignJoinedColumns,
    });

    return arrayJoinClause;
}

////////////////////////////////////////////////////////////////////////////////

// Default name for columns that don't have explicit name in SQL SELECT expressions for syntaxVersion>=3

std::string MakeColumnNameByIndex(int index)
{
    return Format("$projection_%v", index);
}

std::optional<i64> TryGetIntegerValue(NAst::TExpressionPtr expr)
{
    if (auto* literal = expr->As<NAst::TLiteralExpression>()) {
        if (std::holds_alternative<i64>(literal->Value)) {
            return std::get<i64>(literal->Value);
        }
    }

    if (auto* unary = expr->As<NAst::TUnaryOpExpression>()) {
        if (unary->Opcode == EUnaryOp::Minus) {
            if (auto* innerLiteral = unary->Operand[0]->As<NAst::TLiteralExpression>()) {
                if (std::holds_alternative<i64>(innerLiteral->Value)) {
                    return -std::get<i64>(innerLiteral->Value);
                }
            }
        }
    }

    return std::nullopt;
}

class TCardinalityIntoHyperLogLogWithPrecisionRewriter
    : public NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>
{
public:
    TCardinalityIntoHyperLogLogWithPrecisionRewriter(
        TObjectsHolder* head,
        int precision)
        : NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>(head)
        , Precision_(precision)
    { }

    NAst::TExpressionPtr OnFunction(NAst::TFunctionExpressionPtr functionExpr)
    {
        auto rewritten = NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>::OnFunction(functionExpr)->As<NAst::TFunctionExpression>();
        auto name = rewritten->FunctionName;

        if (name == "cardinality") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_state") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_state", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_merge") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_merge", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_merge_state") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_merge_state", Precision_),
                rewritten->Arguments);
        } else {
            return rewritten;
        }
    }

private:
    const int Precision_;
};

void RewriteCardinalityIntoHyperLogLogWithPrecision(
    NAst::TQuery* ast,
    NAst::TAliasMap* aliasMap,
    TObjectsHolder* holder,
    int hyperLogLogPrecision)
{
    auto rewriter = TCardinalityIntoHyperLogLogWithPrecisionRewriter(holder, hyperLogLogPrecision);

    if (auto* subquery = std::get_if<NAst::TQueryAstHeadPtr>(&ast->FromClause); subquery) {
        RewriteCardinalityIntoHyperLogLogWithPrecision(
            &subquery->Get()->Ast,
            &subquery->Get()->AliasMap,
            subquery->Get(),
            hyperLogLogPrecision);
    }

    if (auto* expressions = std::get_if<NAst::TExpressionList>(&ast->FromClause); expressions) {
        for (auto& item : *expressions) {
            item = rewriter.Visit(item);
        }
    }

    for (auto& [key, value] : *aliasMap) {
        value = rewriter.Visit(value);
    }

    if (ast->SelectExprs) {
        ast->SelectExprs = rewriter.Visit(ast->SelectExprs);
    }

    if (ast->WherePredicate) {
        ast->WherePredicate = rewriter.Visit(ast->WherePredicate);
    }

    if (ast->GroupExprs) {
        ast->GroupExprs = rewriter.Visit(ast->GroupExprs);
    }

    if (ast->HavingPredicate) {
        ast->HavingPredicate = rewriter.Visit(ast->HavingPredicate);
    }

    for (auto& item : ast->OrderExpressions) {
        item.Expressions = rewriter.Visit(item.Expressions);
    }
}

void RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeeded(
    NAst::TQuery& ast,
    NAst::TAliasMap& aliasMap,
    NAst::TAstHead& head)
{
    if (!ast.GroupExprs && ast.OrderExpressions.empty()) {
        return;
    }

    bool hasIndexReference = false;

    auto& orderExpressionList = ast.OrderExpressions;
    auto& projections = ast.SelectExprs;
    int projectionCount = projections ? std::ssize(*projections) : 0;

    auto isIndexReference = [&] (NAst::TExpressionPtr expr) {
        auto integerValue = TryGetIntegerValue(expr);

        if (integerValue.has_value() && *integerValue != 0) {
            if (*integerValue < 0 || *integerValue > projectionCount) {
                THROW_ERROR_EXCEPTION("Reference expression index is out of bounds")
                    << TErrorAttribute("index", *integerValue);
            }

            return true;
        }

        return false;
    };

    if (ast.GroupExprs) {
        for (auto* expr : ast.GroupExprs.value()) {
            hasIndexReference = isIndexReference(expr);
        }
    }

    THROW_ERROR_EXCEPTION_IF(hasIndexReference && !projections.has_value(),
        "Projections are not specified, "
        "but projection indices are used in GROUP BY key");

    for (auto& orderExpr : orderExpressionList) {
        for (auto* expr : orderExpr.Expressions) {
            hasIndexReference = isIndexReference(expr);
        }
    }

    THROW_ERROR_EXCEPTION_IF(hasIndexReference && !projections.has_value(),
        "Projections are not specified, "
        "but projection indices are used in ORDER BY key");

    if (!hasIndexReference) {
        return;
    }

    auto indexToAlias = THashMap<i64, std::string>();

    for (i64 projectionIndex = 0; projectionIndex < std::ssize(*projections); ++projectionIndex) {
        auto& expr = (*projections)[projectionIndex];
        auto aliasExpressionAst = expr->As<NAst::TAliasExpression>();

        // NB: Here the SQL standard uses one-based indexing.
        i64 adjustedProjectionIndex = projectionIndex + 1;

        if (aliasExpressionAst) {
            indexToAlias[adjustedProjectionIndex] = aliasExpressionAst->Name;
            continue;
        }

        auto referenceExpressionAst = expr->As<NAst::TReferenceExpression>();
        if (referenceExpressionAst) {
            indexToAlias[adjustedProjectionIndex] = referenceExpressionAst->Reference.ColumnName;
            continue;
        }

        auto newAlias = MakeColumnNameByIndex(projectionIndex + 1);
        indexToAlias[adjustedProjectionIndex] = newAlias;
        aliasMap[newAlias] = expr;

        auto* newExpr = head.New<NAst::TAliasExpression>(expr->SourceLocation, expr, newAlias);
        (*projections)[projectionIndex] = newExpr;
    }

    if (ast.GroupExprs) {
        NAst::TExpressionList& groupExpressionList = *ast.GroupExprs;

        for (i64 index = 0; index < std::ssize(groupExpressionList); ++index) {
            auto& expr = groupExpressionList[index];
            auto integerValue = TryGetIntegerValue(expr);

            if (integerValue.has_value()) {
                auto& aliasName = indexToAlias[*integerValue];
                auto* newExpr = head.New<NAst::TReferenceExpression>(
                    expr->SourceLocation,
                    aliasName);
                groupExpressionList[index] = newExpr;
            }
        }
    }

    for (auto& orderExpr : orderExpressionList) {
        for (i64 index = 0; index < std::ssize(orderExpr.Expressions); ++index) {
            auto& expr = orderExpr.Expressions[index];
            auto integerValue = TryGetIntegerValue(expr);

            if (integerValue.has_value()) {
                auto& aliasName = indexToAlias[*integerValue];
                auto* newExpr = head.New<NAst::TReferenceExpression>(
                    expr->SourceLocation,
                    aliasName);
                orderExpr.Expressions[index] = newExpr;
            }
        }
    }

    return;
}

TPlanFragmentPtr PreparePlanFragmentImpl(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const NAst::TQuery& queryAst,
    const NAst::TAliasMap& aliasMap,
    EExecutionBackend executionBackend,
    int builderVersion,
    IMemoryUsageTrackerPtr memoryTracker,
    int depth)
{
    auto query = New<TQuery>(TGuid::Create());

    auto Logger = MakeQueryLogger(query);

    auto fragment = New<TPlanFragment>();

    Visit(queryAst.FromClause,
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            fragment->SubqueryFragment = PreparePlanFragmentImpl(
                callbacks,
                source,
                subquery->Ast,
                subquery->AliasMap,
                executionBackend,
                builderVersion,
                memoryTracker,
                depth + 1);
        },
        [&] (const NAst::TTableDescriptor&) { },
        [&] (const NAst::TExpressionList&) {
            THROW_ERROR_EXCEPTION("Unexpected expression in from clause");
        });

    auto functions = New<TTypeInferrerMap>();
    callbacks->FetchFunctions(ExtractFunctionNames(queryAst, aliasMap), functions, executionBackend);

    const auto* table = std::get_if<NAst::TTableDescriptor>(&queryAst.FromClause);

    YT_LOG_DEBUG("Getting initial data splits (PrimaryPath: %v, ForeignPaths: %v, SubqueryDepth: %v)",
        table ? table->Path : "unapplicable",
        MakeFormattableView(
            queryAst.Joins,
            [] (TStringBuilderBase* builder, const std::variant<NAst::TJoin, NAst::TArrayJoin>& join) {
                if (auto* tableJoin = std::get_if<NAst::TJoin>(&join)) {
                    FormatValue(builder, tableJoin->Table.Path, TStringBuf());
                }
        }),
        depth);

    std::vector<TFuture<TDataSplit>> asyncDataSplits;
    asyncDataSplits.reserve(queryAst.Joins.size() + 1);
    if (table) {
        asyncDataSplits.push_back(callbacks->GetInitialSplit(table->Path));
    }
    for (const auto& join : queryAst.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                asyncDataSplits.push_back(callbacks->GetInitialSplit(tableJoin.Table.Path));
            },
            [&] (const NAst::TArrayJoin& /*arrayJoin*/) { });
    }

    auto dataSplits = WaitForFast(AllSucceeded(asyncDataSplits))
        .ValueOrThrow();

    YT_LOG_DEBUG("Initial data splits received");

    if (table) {
        fragment->DataSource.ObjectId = dataSplits[0].ObjectId;
        query->Schema.Original = dataSplits[0].TableSchema;
    } else {
        query->Schema.Original = fragment->SubqueryFragment->Query->GetTableSchema();
    }

    auto alias = Visit(queryAst.FromClause,
        [&] (const NAst::TTableDescriptor& tableDescriptor) {
            return tableDescriptor.Alias;
        },
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            return subquery->Alias;
        },
        [&] (const NAst::TExpressionList&) -> std::optional<std::string> {
            return std::nullopt;
        });

    auto builder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap,
        builderVersion);

    builder->AddTable({
        .Schema = *query->Schema.Original,
        .Alias = alias,
        .Mapping = &query->Schema.Mapping,
    });

    std::vector<TJoinClausePtr> joinClauses;
    size_t commonKeyPrefix = std::numeric_limits<size_t>::max();
    int splitIndex = table ? 1 : 0;
    for (const auto& join : queryAst.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                joinClauses.push_back(BuildJoinClause(
                    dataSplits[splitIndex++],
                    tableJoin,
                    source,
                    aliasMap,
                    functions,
                    &commonKeyPrefix,
                    query->Schema.Original,
                    table->Alias,
                    builder.get(),
                    builderVersion,
                    Logger));
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                joinClauses.push_back(BuildArrayJoinClause(
                    arrayJoin,
                    source,
                    aliasMap,
                    functions,
                    builder.get(),
                    builderVersion));
            });
    }

    PrepareQuery(query, queryAst, builder.get());

    // Why after PrepareQuery? GetTableSchema is called inside PrepareQuery?
    query->JoinClauses.assign(joinClauses.begin(), joinClauses.end());

    if (std::ssize(query->JoinClauses) > MaxJoinNumber) {
        THROW_ERROR_EXCEPTION("The number of joins exceeds the allowed maximum. Consider rewriting the query.")
            << TErrorAttribute("join_number", std::ssize(query->JoinClauses))
            << TErrorAttribute("max_join_number", MaxMultiJoinGroupNumber);
    }

    auto joinGroups = GetJoinGroups(query->JoinClauses, query->GetRenamedSchema());
    if (std::ssize(joinGroups) > MaxMultiJoinGroupNumber) {
        THROW_ERROR_EXCEPTION("The number of multi-join groups exceeds the allowed maximum. Consider rewriting the query.")
            << TErrorAttribute("multi_join_group_number", std::ssize(joinGroups))
            << TErrorAttribute("max_multi_join_group_number", MaxMultiJoinGroupNumber);
    }

    if (queryAst.Limit) {
        if (*queryAst.Limit > MaxQueryLimit) {
            THROW_ERROR_EXCEPTION("Maximum LIMIT exceeded")
                << TErrorAttribute("limit", *queryAst.Limit)
                << TErrorAttribute("max_limit", MaxQueryLimit);
        }

        query->Limit = *queryAst.Limit;

        if (!query->OrderClause && query->HavingClause) {
            THROW_ERROR_EXCEPTION("HAVING with LIMIT is not allowed");
        }
    } else if (!queryAst.OrderExpressions.empty()) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    if (queryAst.Offset) {
        if (!query->OrderClause && query->HavingClause) {
            THROW_ERROR_EXCEPTION("HAVING with OFFSET is not allowed");
        }

        query->Offset = *queryAst.Offset;

        if (!queryAst.Limit) {
            THROW_ERROR_EXCEPTION("OFFSET used without LIMIT");
        }
    }

    TryPushDownGroupBy(query, queryAst, Logger);

    auto readSchema = query->GetReadSchema();

    if (fragment->SubqueryFragment) {
        EliminateRedundantProjections(fragment->SubqueryFragment->Query, *readSchema);
    }

    auto queryFingerprint = InferName(query, {.OmitValues = true});
    YT_LOG_DEBUG("Prepared query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v)",
        queryFingerprint,
        *readSchema,
        *query->GetTableSchema());

    auto rowBuffer = New<TRowBuffer>(
        TQueryPreparerBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        memoryTracker);

    fragment->Query = query;

    return fragment;
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    EExecutionBackend executionBackend,
    NYson::TYsonStringBuf placeholderValues,
    int syntaxVersion,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto parsedSource = ParseSource(source, EParseMode::Query, placeholderValues, syntaxVersion);

    return PreparePlanFragment(
        callbacks,
        source,
        std::get<NAst::TQuery>(parsedSource->AstHead.Ast),
        parsedSource->AstHead,
        executionBackend,
        /*builderVersion*/ 1,
        std::move(memoryTracker),
        syntaxVersion);
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NAst::TQuery& queryAst,
    NAst::TAstHead& astHead,
    EExecutionBackend executionBackend,
    int builderVersion,
    IMemoryUsageTrackerPtr memoryTracker,
    int syntaxVersion,
    bool shouldRewriteCardinalityIntoHyperLogLog,
    int hyperLogLogPrecision,
    int depth)
{
    auto aliasMap = astHead.AliasMap;

    if (syntaxVersion >= 3) {
        RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeeded(queryAst, aliasMap, astHead);
    }

    if (shouldRewriteCardinalityIntoHyperLogLog) {
        RewriteCardinalityIntoHyperLogLogWithPrecision(&queryAst, &aliasMap, &astHead, hyperLogLogPrecision);
    }

    return PreparePlanFragmentImpl(
        callbacks,
        source,
        queryAst,
        aliasMap,
        executionBackend,
        builderVersion,
        std::move(memoryTracker),
        depth);
}

TQueryPtr PrepareJobQuery(
    TStringBuf source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher)
{
    auto astHead = ParseQueryString(source, NAst::TParser::token::StrayWillParseJobQuery);
    const auto& ast = std::get<NAst::TQuery>(astHead.Ast);
    const auto& aliasMap = astHead.AliasMap;

    if (ast.Offset) {
        THROW_ERROR_EXCEPTION("OFFSET is not supported in map-reduce queries");
    }

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto query = New<TQuery>(TGuid::Create());
    query->Schema.Original = tableSchema;

    auto functionNames = ExtractFunctionNames(ast, aliasMap);

    auto functions = New<TTypeInferrerMap>();
    functionsFetcher(functionNames, functions, EExecutionBackend::Native);

    auto builder = CreateExpressionBuilder(source, functions, aliasMap, 1);

    builder->AddTable({
        .Schema = *tableSchema,
        .Alias = std::nullopt,
        .Mapping = &query->Schema.Mapping,
    });

    PrepareQuery(
        query,
        ast,
        builder.get());

    return query;
}

TConstExpressionPtr PrepareExpression(
    TStringBuf source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    return PrepareExpression(
        *ParseSource(source, EParseMode::Expression),
        tableSchema,
        1,
        functions,
        references);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions)
{
    return PrepareExpression(parsedSource, tableSchema, 1, functions);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    int builderVersion,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    auto expr = std::get<NAst::TExpressionPtr>(parsedSource.AstHead.Ast);
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    std::vector<TColumnDescriptor> mapping;

    auto builder = CreateExpressionBuilder(parsedSource.Source, functions, aliasMap, builderVersion);

    builder->AddTable({
        .Schema = tableSchema,
        .Alias = std::nullopt,
        .Mapping = &mapping,
    });

    auto result = ApplyRewriters(builder->BuildTypedExpression(expr));

    if (references) {
        for (const auto& item : mapping) {
            references->insert(item.Name);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
