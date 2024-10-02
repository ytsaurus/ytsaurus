#include "select_continuation.h"
#include "helpers.h"

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/query/base/helpers.h>

#include <functional>

namespace NYT::NOrm::NServer::NObjects {

using namespace NQueryClient::NAst;

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void DoFormatValue(
    TStringBuilderBase* builder,
    const TSelectContinuationTokenEntry& entry)
{
    if (entry.ObjectExpression.empty()) {
        builder->AppendFormat("DBExpression: %v, DBTableName: %v, DBEvaluated: %v, Descending: %v",
            entry.DBExpression,
            entry.DBTableName,
            entry.DBEvaluated,
            entry.Descending);
    } else {
        builder->AppendFormat("ObjectExpression: %v, Descending: %v",
            entry.ObjectExpression,
            entry.Descending);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSelectContinuationTokenEntry::TSelectContinuationTokenEntry(TObjectOrderByExpression objectOrderByExpression)
    : ObjectExpression(std::move(objectOrderByExpression.Expression))
    , Descending(objectOrderByExpression.Descending)
{ }

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectContinuationTokenEntry& entry,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{");
    DoFormatValue(builder, entry);
    builder->AppendFormat("}");
}

void TSelectContinuationTokenEntry::ValidateExactlyOneExpression() const
{
    THROW_ERROR_EXCEPTION_IF(ObjectExpression.size() > 0 && DBExpression.size() > 0,
        "Select continuation token entry must contain exactly one of object and DB expressions, "
        "but actually contains both");
    THROW_ERROR_EXCEPTION_IF(ObjectExpression.size() == 0 && DBExpression.size() == 0,
        "Select continuation token entry must contain exactly one of object and DB expressions, "
        "but actually contains neither of them");
    THROW_ERROR_EXCEPTION_IF(DBExpression.empty() != DBTableName.empty(),
        "Select continuation token entry must contain DB expression and table name both specified, "
        "or both empty, but got DB expression %Qv and DB table name %Qv",
        DBExpression,
        DBTableName);
}

////////////////////////////////////////////////////////////////////////////////

TSelectContinuationTokenEntry& TSelectContinuationTokenEvaluatedEntry::AsNotEvaluated()
{
    return *this;
}

const TSelectContinuationTokenEntry& TSelectContinuationTokenEvaluatedEntry::AsNotEvaluated() const
{
    return *this;
}

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectContinuationTokenEvaluatedEntry& evaluatedEntry,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{");
    DoFormatValue(builder, evaluatedEntry.AsNotEvaluated());
    builder->AppendFormat(", YsonValue: %Qv}",
        evaluatedEntry.YsonValue);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectsContinuationToken& continuationToken,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("MajorVersion: %v", continuationToken.MajorVersion);
    wrapper->AppendFormat("MinorVersion: %v", continuationToken.MinorVersion);
    wrapper->AppendFormat("EvaluatedEntries: %v", continuationToken.EvaluatedEntries);
    wrapper->AppendFormat("SerializedToken: %v", continuationToken.SerializedToken);
    builder->AppendChar('}');
}

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryContinuationToken& continuationToken,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder, ", ");

    builder->AppendChar('{');
    wrapper->AppendFormat("MajorVersion: %v", continuationToken.MajorVersion);
    wrapper->AppendFormat("MinorVersion: %v", continuationToken.MinorVersion);
    wrapper->AppendFormat("ReadPhase: %v", continuationToken.ReadPhase);
    wrapper->AppendFormat("ReadSource: %v", continuationToken.ReadSource);
    wrapper->AppendFormat("EvaluatedEntries: %v", continuationToken.EvaluatedEntries);
    wrapper->AppendFormat("SerializedToken: %v", continuationToken.SerializedToken);
    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

void ValidateSelectContinuationTokenReceivedEntries(
    const std::vector<TSelectContinuationTokenEvaluatedEntry>& receivedEvaluatedEntries,
    const std::vector<TSelectContinuationTokenEntry>& expectedEntries)
{
    // Continuation token can be empty after a select with no output rows.
    if (receivedEvaluatedEntries.empty()) {
        return;
    }
    if (receivedEvaluatedEntries.size() != expectedEntries.size()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
            "Invalid continuation token: expected %v entries, but got %v",
            expectedEntries.size(),
            receivedEvaluatedEntries.size());
    }
    for (size_t i = 0; i < expectedEntries.size(); ++i) {
        if (receivedEvaluatedEntries[i].AsNotEvaluated() != expectedEntries[i]) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
                "Invalid continuation token: expected entry %v at index %v, but got %v",
                expectedEntries[i],
                i,
                receivedEvaluatedEntries[i].AsNotEvaluated());
        }
    }
}

TLiteralValue ConvertSelectContinuationTokenEntryValueToLiteral(
    const NYson::TYsonString& value)
{
    try {
        auto node = ConvertToNode(value);
        auto type = node->GetType();

        switch (type) {
            case ENodeType::Boolean:
                return node->AsBoolean()->GetValue();
            case ENodeType::Double:
                return node->AsDouble()->GetValue();
            case ENodeType::Int64:
                return node->AsInt64()->GetValue();
            case ENodeType::Uint64:
                return node->AsUint64()->GetValue();
            case ENodeType::String:
                return node->AsString()->GetValue();
            case ENodeType::Entity:
                return TNullLiteralValue{};
            case ENodeType::List:
            case ENodeType::Map:
            case ENodeType::Composite:
                THROW_ERROR_EXCEPTION("Unsupported type %Qv of select continuation token entry value",
                    type);
            default:
                YT_ABORT();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing select continuation token entry value")
            << ex;
    }
}

NQueryClient::NAst::TExpressionPtr CreateContinuationTokenFilterExpression(
    const std::vector<TSelectContinuationTokenEvaluatedEntry>& evaluatedEntries,
    const std::vector<NQueryClient::NAst::TExpressionPtr>& entryExpressions,
    TObjectsHolder* holder)
{
    using NYT::NQueryClient::EBinaryOp;
    using NYT::NQueryClient::TSourceLocation;
    using namespace NQueryClient::NAst;

    if (evaluatedEntries.empty()) {
        return nullptr;
    }

    auto createComparisonExpression = [&] (EBinaryOp operation, TExpressionList lhs, TExpressionList rhs) {
        return holder->New<TBinaryOpExpression>(
            TSourceLocation(),
            operation,
            std::move(lhs),
            std::move(rhs));
    };
    auto createLiteralExpression = [&] (const TLiteralValue& value) {
        return holder->New<TLiteralExpression>(TSourceLocation(), value);
    };

    bool allDescending = true;
    bool allAscending = true;
    for (const auto& evaluatedEntry : evaluatedEntries) {
        if (evaluatedEntry.Descending) {
            allAscending = false;
        } else {
            allDescending = false;
        }
    }
    if (allAscending || allDescending) {
        TExpressionList lhs;
        TExpressionList rhs;
        for (int index = 0; index < std::ssize(evaluatedEntries); ++index) {
            const auto& evaluatedEntry = evaluatedEntries[index];
            auto value = ConvertSelectContinuationTokenEntryValueToLiteral(evaluatedEntry.YsonValue);
            auto expression = entryExpressions[index];
            lhs.push_back(std::move(expression));
            rhs.push_back(createLiteralExpression(value));
        }
        return createComparisonExpression(allAscending ? EBinaryOp::Greater : EBinaryOp::Less,
            std::move(lhs),
            std::move(rhs));
    }

    TExpressionPtr resultExpression = nullptr;
    TExpressionPtr previousEqualExpression = nullptr;
    for (int index = 0; index < std::ssize(evaluatedEntries); ++index) {
        const auto& evaluatedEntry = evaluatedEntries[index];
        auto value = ConvertSelectContinuationTokenEntryValueToLiteral(evaluatedEntry.YsonValue);
        auto expression = entryExpressions[index];
        auto currentEqualExpression = createComparisonExpression(
            EBinaryOp::Equal,
            TExpressionList{expression},
            TExpressionList{createLiteralExpression(value)});
        auto currentNotEqualExpression = createComparisonExpression(
            evaluatedEntry.Descending ? EBinaryOp::Less : EBinaryOp::Greater,
            TExpressionList{expression},
            TExpressionList{createLiteralExpression(value)});
        auto currentNotEqualPreviousEqualExpression = NQueryClient::BuildAndExpression(
            holder,
            currentNotEqualExpression,
            previousEqualExpression);
        resultExpression = NQueryClient::BuildOrExpression(
            holder,
            resultExpression,
            currentNotEqualPreviousEqualExpression);
        previousEqualExpression = NQueryClient::BuildAndExpression(
            holder,
            previousEqualExpression,
            currentEqualExpression);
    }

    return resultExpression;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
