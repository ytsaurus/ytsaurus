#pragma once

#include "order.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TSelectContinuationTokenEntry
{
    TSelectContinuationTokenEntry() = default;
    explicit TSelectContinuationTokenEntry(TObjectOrderByExpression objectOrderByExpression);

    bool operator == (const TSelectContinuationTokenEntry& other) const = default;

    void ValidateExactlyOneExpression() const;

    // Either |ObjectExpression| or |DBExpression|+|DBTableName| must be specified.
    //
    // |DBExpression| alone is not enough, because some object expressions
    // cannot be represented as a DB expression, for example, evaluated attributes.
    //
    // |ObjectExpression|+|DBTableName| alone is not enough, because some DB columns are
    // not integrated with the attribute model, for example, hash columns,
    // which are used for uniform data distribution.

    // ORM query.
    // Examples: `[/meta/id]`, `[/meta/creation_time] + 100`.
    TString ObjectExpression;

    // Dynamic table query.
    // Examples: `[meta.id]`, `[meta.creation_time]`.
    TString DBExpression;

    // Concrete table from which the selection is performed.
    // Examples: `books`, `books_by_year`
    TString DBTableName;

    // True if |DBExpression| refers to evaluated fields only.
    bool DBEvaluated = false;

    bool Descending = false;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectContinuationTokenEntry& entry,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSelectContinuationTokenEvaluatedEntry
    : public TSelectContinuationTokenEntry
{
    TSelectContinuationTokenEntry& AsNotEvaluated();
    const TSelectContinuationTokenEntry& AsNotEvaluated() const;

    NYson::TYsonString YsonValue;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectContinuationTokenEvaluatedEntry& evaluatedEntry,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectsContinuationToken
{
    int MajorVersion;
    int MinorVersion;
    std::vector<TSelectContinuationTokenEvaluatedEntry> EvaluatedEntries;
    TString SerializedToken;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectsContinuationToken& continuationToken,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHistoryReadPhase,
    ((Uninitialized)        (-2))
    ((IndexAscendingBorder) (-1))
    ((Main)                  (0))
    ((IndexDescendingBorder) (1))
);

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryContinuationTokenSchema
{
    const THistoryTableBase* ReadSource;
    std::vector<TSelectContinuationTokenEntry> Entries;
};

struct TSelectObjectHistoryContinuationToken
{
    int MajorVersion;
    int MinorVersion;
    EHistoryReadPhase ReadPhase;
    TString ReadSource;
    std::vector<TSelectContinuationTokenEvaluatedEntry> EvaluatedEntries;
    TString SerializedToken;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectObjectHistoryContinuationToken& continuationToken,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

void ValidateSelectContinuationTokenReceivedEntries(
    const std::vector<TSelectContinuationTokenEvaluatedEntry>& receivedEvaluatedEntries,
    const std::vector<TSelectContinuationTokenEntry>& expectedEntries);

NQueryClient::NAst::TLiteralValue ConvertSelectContinuationTokenEntryValueToLiteral(
    const NYT::NYson::TYsonString& value);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr CreateContinuationTokenFilterExpression(
    const std::vector<TSelectContinuationTokenEvaluatedEntry>& evaluatedEntries,
    const std::vector<NQueryClient::NAst::TExpressionPtr>& entryExpressions,
    TObjectsHolder* holder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
