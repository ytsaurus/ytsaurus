#include "query_preparer.h"

#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "lexer.h"
#include "private.h"
#include "push_down_group_by.h"
#include "query_helpers.h"
#include "query_visitors.h"
#include "expr_builder_base.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/misc/variant.h>

#include <unordered_set>

namespace NYT::NQueryClient {

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
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions);

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& exprs,
    std::vector<TString>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<TString>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(to_lower(functionExpr->FunctionName));
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
    } else {
        YT_ABORT();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<TString>* functions)
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
    std::vector<TString>* functions)
{
    CheckStackDepth();

    for (const auto& [condition, result] : whenThenExpressions) {
        ExtractFunctionNames(condition, functions);
        ExtractFunctionNames(result, functions);
    }
}

std::vector<TString> ExtractFunctionNames(
    const NAst::TQuery& query,
    const NAst::TAliasMap& aliasMap)
{
    std::vector<TString> functions;

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
    TExprBuilder* builder,
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

    return typedPredicate;
}

TGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    TExprBuilder* builder)
{
    auto groupClause = New<TGroupClause>();
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder->BuildTypedExpression(expressionAst, ComparableTypes);
        groupClause->AddGroupItem(typedExpr, builder->InferGroupItemName(typedExpr, *expressionAst));
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
    TExprBuilder* builder)
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

                orderClause->OrderItems.push_back({typedExpr, orderExpr.Descending});
            }
        }

        ssize_t keyPrefix = 0;
        while (keyPrefix < std::ssize(orderClause->OrderItems)) {
            const auto& item = orderClause->OrderItems[keyPrefix];

            if (item.Descending) {
                break;
            }

            const auto* referenceExpr = item.Expression->As<TReferenceExpression>();

            if (!referenceExpr) {
                break;
            }

            auto columnIndex = ColumnNameToKeyPartIndex(query->GetKeyColumns(), referenceExpr->ColumnName);

            if (keyPrefix != columnIndex) {
                break;
            }
            ++keyPrefix;
        }

        if (keyPrefix < std::ssize(orderClause->OrderItems)) {
            query->OrderClause = std::move(orderClause);
        }

        // Use ordered scan otherwise
    }

    if (ast.SelectExprs) {
        auto projectClause = New<TProjectClause>();
        for (const auto& expressionAst : *ast.SelectExprs) {
            auto typedExpr = builder->BuildTypedExpression(expressionAst);

            projectClause->AddProjection(typedExpr, InferColumnName(*expressionAst));
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

THashMap<TString, TString> ConvertYsonPlaceholdersToQueryLiterals(TYsonStringBuf placeholders)
{
    TMemoryInput input{placeholders.AsStringBuf()};
    TYsonPullParser ysonParser{&input, EYsonType::Node};
    TYsonPullParserCursor ysonCursor{&ysonParser};

    if (ysonCursor->GetType() != EYsonItemType::BeginMap) {
        YsonParseError("Incorrect placeholder argument: YSON map expected", placeholders);
    }

    ysonCursor.Next();

    THashMap<TString, TString> queryLiterals;
    while (ysonCursor->GetType() != EYsonItemType::EndMap) {
        if (ysonCursor->GetType() != EYsonItemType::StringValue) {
            YsonParseError("Incorrect YSON map placeholder: keys should be strings", placeholders);
        }
        auto key = TString(ysonCursor->UncheckedAsString());

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

    THashMap<TString, TString> queryLiterals;
    if (placeholderValues) {
        queryLiterals = ConvertYsonPlaceholdersToQueryLiterals(placeholderValues);
    }

    NAst::TLexer lexer(source, strayToken, std::move(queryLiterals), syntaxVersion);
    NAst::TParser parser(lexer, &head, source, /*aliasMapStack*/ {});

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(TRange<TString> /*names*/, const TTypeInferrerMapPtr& typeInferrers)
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
            auto [_, inserted] = map.insert(std::make_pair(ref->ColumnName, index));
            THROW_ERROR_EXCEPTION_IF(!inserted, "Foreign key column %Qv occurs more than once in a join clause",
                ref->ColumnName);
        }
    }

    return map;
}

std::vector<std::pair<TConstExpressionPtr, int>> MakeExpressionsFromComputedColumns(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& functions,
    const std::optional<TString>& alias)
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
            TAddAliasRewriter addAliasRewriter{.Alias=alias};
            evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
        }

        expressionsAndColumnIndices.push_back({std::move(evaluatedColumnExpression), index});
    }

    return expressionsAndColumnIndices;
}

TJoinClausePtr BuildJoinClause(
    const TDataSplit& foreignDataSplit,
    const NAst::TJoin& tableJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    size_t* globalCommonKeyPrefix,
    const TTableSchemaPtr& tableSchema,
    const std::optional<TString>& tableAlias,
    TExprBuilder* builder,
    const NLogging::TLogger& Logger)
{
    auto foreignTableSchema = foreignDataSplit.TableSchema;

    auto joinClause = New<TJoinClause>();
    joinClause->Schema.Original = foreignTableSchema;
    joinClause->ForeignObjectId = foreignDataSplit.ObjectId;
    joinClause->IsLeft = tableJoin.IsLeft;

    // BuildPredicate and BuildTypedExpression are used with foreignBuilder.
    auto foreignBuilder = CreateExpressionBuilder(source, functions, aliasMap);

    foreignBuilder->AddTable({
        *joinClause->Schema.Original,
        tableJoin.Table.Alias,
        &joinClause->Schema.Mapping});

    std::vector<TSelfEquation> selfEquations;
    selfEquations.reserve(tableJoin.Fields.size() + tableJoin.Lhs.size());
    std::vector<TConstExpressionPtr> foreignEquations;
    foreignEquations.reserve(tableJoin.Fields.size() + tableJoin.Rhs.size());

    THashSet<std::string> commonColumnNames;
    // Merge columns.
    for (const auto& referenceExpr : tableJoin.Fields) {
        auto columnName = InferReferenceName(referenceExpr->Reference);
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

        selfEquations.push_back({
            .Expression=New<TReferenceExpression>(selfColumnType, columnName),
            .Evaluated=false,
        });
        foreignEquations.push_back(New<TReferenceExpression>(foreignColumnType, columnName));
    }

    for (const auto& argument : tableJoin.Lhs) {
        selfEquations.push_back({
            .Expression=builder->BuildTypedExpression(argument, ComparableTypes),
            .Evaluated=false,
        });
    }
    for (const auto& argument : tableJoin.Rhs) {
        foreignEquations.push_back(foreignBuilder->BuildTypedExpression(argument, ComparableTypes));
    }

    if (selfEquations.size() != foreignEquations.size()) {
        THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
            selfEquations.size(),
            foreignEquations.size())
            << TErrorAttribute("lhs_source", FormatExpression(tableJoin.Lhs))
            << TErrorAttribute("rhs_source", FormatExpression(tableJoin.Rhs));
    }

    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (selfEquations[index].Expression->GetWireType() != foreignEquations[index]->GetWireType()) {
            THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                InferName(selfEquations[index].Expression),
                InferName(foreignEquations[index]))
                << TErrorAttribute("self_type", selfEquations[index].Expression->LogicalType)
                << TErrorAttribute("foreign_type", foreignEquations[index]->LogicalType);
        }
    }

    // If possible, use ranges, rearrange equations according to foreign key columns, enriching with evaluated columns
    size_t commonKeyPrefix = 0;
    size_t foreignKeyPrefix = 0;
    std::vector<TSelfEquation> keySelfEquations;
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
                TAddAliasRewriter addAliasRewriter{.Alias=*alias};
                evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
            }
            TSelfifyRewriter selfifyRewriter{
                .SelfEquations=selfEquations,
                .ForeignReferenceToIndexMap=foreignReferenceToIndexMap,
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

            keySelfEquations.push_back({
                .Expression=std::move(matchingSelfExpression),
                .Evaluated=false,
            });
            keyForeignEquations.push_back(New<TReferenceExpression>(
                foreignKeyColumn.LogicalType(),
                aliasedForeignKeyColumnName));
        } else {
            break;
        }

        if (commonKeyPrefix == foreignKeyPrefix &&
            static_cast<int>(commonKeyPrefix) < tableSchema->GetKeyColumnCount())
        {
            if (auto* reference = keySelfEquations.back().Expression->As<TReferenceExpression>()) {
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

    builder->AddTable({
        *joinClause->Schema.Original,
        tableJoin.Table.Alias,
        &joinClause->Schema.Mapping,
        &joinClause->SelfJoinedColumns,
        &joinClause->ForeignJoinedColumns,
        commonColumnNames});

    return joinClause;
}

TJoinClausePtr BuildArrayJoinClause(
    const NAst::TArrayJoin& arrayJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    TExprBuilder* builder)
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
                builder->BuildTypedExpression(
                    aliasExpression->Expression,
                    {EValueType::Composite});

        auto logicalType = typedExpression->LogicalType;
        auto metatype = logicalType->GetMetatype();
        if (metatype == ELogicalMetatype::Optional) {
            logicalType = logicalType->UncheckedAsOptionalTypeRef().GetElement();
            metatype = logicalType->GetMetatype();
        }

        THROW_ERROR_EXCEPTION_IF(metatype != ELogicalMetatype::List,
            "Expected a list-like type expression in the ARRAY JOIN operator, got %v",
            *typedExpression->LogicalType);

        auto containedType = logicalType->UncheckedAsListTypeRef().GetElement();
        nestedColumns[index] = TColumnSchema(aliasExpression->Name, std::move(containedType));
    }

    arrayJoinClause->Schema.Original = New<TTableSchema>(std::move(nestedColumns));

    auto arrayBuilder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap);

    arrayBuilder->AddTable({
        *arrayJoinClause->Schema.Original,
        std::nullopt,
        &arrayJoinClause->Schema.Mapping});

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
        *arrayJoinClause->Schema.Original,
        std::nullopt,
        &arrayJoinClause->Schema.Mapping,
        &arrayJoinClause->SelfJoinedColumns,
        &arrayJoinClause->ForeignJoinedColumns});

    return arrayJoinClause;
}

////////////////////////////////////////////////////////////////////////////////

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    TYsonStringBuf placeholderValues,
    int syntaxVersion,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto parsedSource = ParseSource(source, EParseMode::Query, placeholderValues, syntaxVersion);
    return PreparePlanFragment(
        callbacks,
        parsedSource->Source,
        std::get<NAst::TQuery>(parsedSource->AstHead.Ast),
        parsedSource->AstHead.AliasMap,
        std::move(memoryTracker));
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const NAst::TQuery& queryAst,
    const NAst::TAliasMap& aliasMap,
    IMemoryUsageTrackerPtr memoryTracker,
    int depth)
{
    auto query = New<TQuery>(TGuid::Create());

    auto Logger = MakeQueryLogger(query);

    auto fragment = New<TPlanFragment>();

    Visit(queryAst.FromClause,
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            fragment->SubqueryFragment = PreparePlanFragment(
                callbacks,
                source,
                subquery->Ast,
                subquery->AliasMap,
                memoryTracker,
                depth + 1);
        },
        [&] (const NAst::TTableDescriptor&) { });

    auto functions = New<TTypeInferrerMap>();
    callbacks->FetchFunctions(ExtractFunctionNames(queryAst, aliasMap), functions);

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
        });

    auto builder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap);

    builder->AddTable({
        *query->Schema.Original,
        alias,
        &query->Schema.Mapping});

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
                    Logger));
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                joinClauses.push_back(BuildArrayJoinClause(
                    arrayJoin,
                    source,
                    aliasMap,
                    functions,
                    builder.get()));
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
    functionsFetcher(functionNames, functions);

    auto builder = CreateExpressionBuilder(source, functions, aliasMap);

    builder->AddTable({
        *tableSchema, std::nullopt, &query->Schema.Mapping});

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
        functions,
        references);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    auto expr = std::get<NAst::TExpressionPtr>(parsedSource.AstHead.Ast);
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    std::vector<TColumnDescriptor> mapping;

    auto builder = CreateExpressionBuilder(parsedSource.Source, functions, aliasMap);

    builder->AddTable({
        tableSchema, std::nullopt, &mapping});

    auto result = builder->BuildTypedExpression(expr);

    if (references) {
        for (const auto& item : mapping) {
            references->insert(item.Name);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
